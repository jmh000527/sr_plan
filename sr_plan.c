/*
 * sr_plan.c
 *
 * sr_plan (Save and Restore Plan) 是一个 PostgreSQL 扩展，用于保存和恢复查询执行计划。
 * 这个文件的主要功能是通过 planner_hook 拦截执行计划生成过程，
 * 当设置为 write_mode 时将计划保存到系统表，
 * 在常规查询中重用已经保存过的执行计划。
 */
#include "sr_plan.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/hash.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 100000
#include "utils/queryenvironment.h"
#include "catalog/index.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "catalog/pg_extension_d.h"
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(do_nothing);
PG_FUNCTION_INFO_V1(show_plan);
PG_FUNCTION_INFO_V1(_p);

void _PG_init(void);
void _PG_fini(void);

/*
 * 知识点：全局函数指针用于维护 Hook (回调钩子) 链
 *
 * 这两行是和上面的 _PG_init 里提到的“Hook 指针劫持”配套的前置声明保存变量。
 * PostgreSQL 允许同时加载多个能拦截执行流程的插件（不如 pg_stat_statements 等）。
 * 为了避免互相踩踏，标准的行为规范是：
 * 1. 每个人在注册自己的接管函数前，用变量（比如 srplan_planner_hook_next）将当时系统中已存在的 planner_hook 指针保存下来。
 * 2. 自己的处理逻辑做完了之后，再去显式调用被自己保存下来的那个 “next” 指针，
 *    如此就类似形成了一个中间件责任链(Middleware Chain)的形态，共同完成一条 SQL 的加工。
 */
static planner_hook_type srplan_planner_hook_next = NULL;
post_parse_analyze_hook_type srplan_post_parse_analyze_hook_next = NULL;

/* 记录缓存状态的全局变量结构体，用于保存插件的状态和涉及到的各种表/索引的 OID */
typedef struct SrPlanCachedInfo {
	bool	enabled;			/* 插件是否启用 */
	bool	write_mode;			/* 是否开启写模式（保存执行计划） */
	bool	explain_query;		/* 当前是否为 Explain 查询 */
	int		log_usage;			/* 计划使用时的日志记录等级 */
	Oid		fake_func;			/* 伪函数（_p）的 OID，用于查询参数 */
	Oid		schema_oid;			/* sr_plan 的 schema OID */
	Oid		sr_plans_oid;		/* 保存执行计划的表的 OID */
	Oid		sr_index_oid;		/* 执行计划表索引的 OID */
	Oid		reloids_index_oid;	/* 表 OID 索引的 OID */
	Oid		index_reloids_index_oid; /* 索引 OID 的索引的 OID */
	const char* query_text;		/* 当前查询的文本 */
} SrPlanCachedInfo;

/*
 * 输出计划函数的上下文状态结构体。
 * 该结构体用于支持 PostgreSQL 的返回集合函数 (Set Returning Function, SRF)。
 * 当用户通过 SELECT 调用 show_plan 输出被保存的执行计划时，它负责在多次迭代中维护输出状态。
 */
typedef struct show_plan_funcctx {
	ExplainFormat	format;			/* 输出格式，如 TEXT, XML, JSON 等 */
	char* output;					/* 当前剩余未输出的计划字符串内容的指针 */
	int				lines_count;	/* 当前已经处理或输出的行数计数（如果需要分行输出时使用） */
} show_plan_funcctx;

/*
 * 声明并初始化全局受保护的插件缓存。
 * 当 PostgreSQL 启动或新的会话建连时，这些成员将作为默认值被加载。
 * 随后一些参数会通过 GUC(用户配置变量) 或者 init_sr_plan() 函数被赋值。
 *
 * 这个静态全局变量的生命周期跟随着 PostgreSQL 的 Backend 进程（由于 PG 是多进程模型，
 * 每一个新连接的 Session 会拥有独立的内存空间，这份全局变量在每一个 Session 中也都是独立初始化的）
 */
static SrPlanCachedInfo cachedInfo = {
	true,			/* enabled: 默认开启 sr_plan 功能 */
	false,			/* write_mode: 默认关闭写模式，避免将所有查询都保存到系统表 */
	false,			/* explain_query: 当前语句是否为 Explain (初始为 false) */
	0,				/* log_usage: 默认不进行日志记录 (0=none) */
	0,				/* fake_func: 默认无 _p 函数 OID */
	InvalidOid,		/* schema_oid: 初始无效 OID，等待 lazy_init 查找安装的 schema */
	InvalidOid,		/* sr_plans_o id（原注：sr_plans_reloid）: 执行计划主表 OID 初始无效 */
	InvalidOid,		/* sr_index_oid（原注：sr_plans_index_oid）: 主表索引 OID 初始无效 */
	InvalidOid,		/* reloids_index_oid: 关联表 OID 索引的 OID 初始无效 */
	InvalidOid,		/* index_reloids_index_oid: 关联索引 OID 的索引始无效 */
	NULL			/* query_text: 原生查询 SQL 文本指针初始为空 */
};

#if PG_VERSION_NUM >= 130000
static PlannedStmt* sr_planner(Query* parse, const char* query_string,
	int cursorOptions, ParamListInfo boundParams);
#else
static PlannedStmt* sr_planner(Query* parse, int cursorOptions,
	ParamListInfo boundParams);
#endif

static void sr_analyze(ParseState* pstate, Query* query);

static Oid get_sr_plan_schema(void);
static Oid sr_get_relname_oid(Oid schema_oid, const char* relname);
static bool sr_query_walker(Query* node, void* context);
static bool sr_query_expr_walker(Node* node, void* context);
void walker_callback(void* node);
static void sr_plan_relcache_hook(Datum arg, Oid relid);

static void plan_tree_visitor(Plan* plan,
	void (*visitor) (Plan* plan, void* context),
	void* context);
static void execute_for_plantree(PlannedStmt* planned_stmt,
	void (*proc) (void* context, Plan* plan),
	void* context);
static void restore_params(void* context, Plan* plan);
static Datum get_query_hash(Query* node);
static void collect_indexid(void* context, Plan* plan);

/*
 * 查询参数对象结构体。
 * 用于暂存从 `_p()` 函数中提取出的实际参数信息。
 */
struct QueryParam
{
	int location;     /* 在原始 SQL 字符串中的字符位置，用于匹配替换。 */
	int funccollid;   /* 原始伪函数的 Collation OID，因为 location 在某些过程中会丢失，所以在抽象树中临时存入此处。 */
	void* node;       /* 实际绑定的参数表达式节点（通常是 Const 或 Param 节点），供恢复时塞回语法树。 */
};

/*
 * 查询参数上下文结构体。
 * 在遍历查询树（walker）时作为全局状态传递。
 */
struct QueryParamsContext
{
	bool	collect; 	/* 是否正在收集参数，初始为 false */
	List* params; 		/* 收集到的参数列表，每个元素是 QueryParam 结构体 */
};

/*
 * 索引 ID 上下文结构体。
 * 在遍历查询树（walker）时作为全局状态传递。
 */
struct IndexIds
{
	List* ids; /* 收集到的索引 ID 列表，每个元素是 Oid 类型 */
};

/*
 * 全局查询参数列表。
 * 用于存储从查询中提取出的所有参数，每个元素是 QueryParam 结构体。
 */
List* query_params;

/*
 * 清空全局缓存中的相关的系统表 Oid，以便在下次查询时可以重新初始化。
 * 当有关表的数据变动或者插件更新时调用。
 */
static void
invalidate_oids(void)
{
	cachedInfo.schema_oid = InvalidOid;
	cachedInfo.sr_plans_oid = InvalidOid;
	cachedInfo.sr_index_oid = InvalidOid;
	cachedInfo.fake_func = InvalidOid;
	cachedInfo.reloids_index_oid = InvalidOid;
	cachedInfo.index_reloids_index_oid = InvalidOid;
}

/*
 * 初始化 sr_plan，查找和记录 sr_plan 表，索引和用于参数替换的 fake func（_p），
 * 并注册重读相关缓存时的回调函数（sr_plan_relcache_hook）。
 * 只有在需要使用时才会调用此函数以避免启动开销。
 */
static bool
init_sr_plan(void)
{
	char* schema_name;
	List* func_name_list;

	/* 定义查找函数时所需的参数类型，这里是 ANYELEMENTOID */
	Oid args[1] = { ANYELEMENTOID };

	/*
	 * 知识点：静态变量与注册幂等性
	 * 用 static 标记代表这个变量在跨越多次函数调用时仍能记住它的历史状态。
	 * 在这里用于确保我们只注册一次 relation cache 刷新回调（避免重复挂载导致内存/性能泄漏）。
	 */
	static bool relcache_callback_needed = true;

	/*
	 * 获取并缓存本插件所挂载在的 Schema（命名空间）OID，如果还没创建/发生异常则安静地返回撤退
	 */
	cachedInfo.schema_oid = get_sr_plan_schema();
	if (cachedInfo.schema_oid == InvalidOid)
		return false;

	/*
	 * 知识点：系统表反向 OID 查找与缓存架构
	 *
	 * 【问题背景】：
	 * 在 SQL 世界里，我们习惯用“字符串名字”来称呼一张表，比如 "SELECT * FROM sr_plans"。
	 * 但在 PostgreSQL 的 C 语言内核层，为了极致的运行速度，所有对数据库对象（表、索引、视图）的操作
	 * 都不用名字，而是用一个称为 OID（Object ID，本质是一个无符号整数，类似对象的身份证号）来寻址。
	 *
	 * 【性能灾难说明】：
	 * 本插件的作用是“拦截并加速所有的用户 SQL”。如果我们在这个关键的拦截器里，每次都傻傻地拿着
	 * 在头文件里定义的字符串 `"sr_plans_query_hash_idx"`，去向数据库打听：“喂，请问这张表现在的 OID 是多少？”，
	 * 这种跨层查询的开销，甚至比直接执行那句被拦截的 SQL 还要慢得多，就成了捡了芝麻丢了西瓜。
	 *
	 * 【解决方案（当前代码的逻辑）】：
	 * 1. 懒加载触发：这个 `init_sr_plan` 函数只在一个新数据库连接（Session）第一次跑语句时调用一次。
	 * 2. 集中查找：调用 `sr_get_relname_oid` 函数，把那些死死硬编码的字符串表名/索引名，
	 *    转换成 PG 内核能直接处理的数字型 OID（比如把 "sr_plans" 查出等于 16401）。
	 * 3. 结果固化：把这 4 个昂贵查来的数字 OID，全部保存到 `cachedInfo` 这样一个在当前会话
	 *    全局存在的 C 结构体变量中。
	 * 4. 一劳永逸：以后这个会话接下来的几万条查询，只要访问 `cachedInfo.sr_plans_oid` 就能瞬间拿到
	 *    这个表的身份证号，畅通无阻地进行底层的读写了。免去了几万次的字符串解析。
	 */
	cachedInfo.sr_index_oid = sr_get_relname_oid(cachedInfo.schema_oid,
		SR_PLANS_TABLE_QUERY_INDEX_NAME);
	cachedInfo.sr_plans_oid = sr_get_relname_oid(cachedInfo.schema_oid,
		SR_PLANS_TABLE_NAME);
	cachedInfo.reloids_index_oid = sr_get_relname_oid(cachedInfo.schema_oid,
		SR_PLANS_RELOIDS_INDEX);
	cachedInfo.index_reloids_index_oid = sr_get_relname_oid(cachedInfo.schema_oid,
		SR_PLANS_INDEX_RELOIDS_INDEX);

	/*
	 * 校验异常并容错：
	 * 为什么会有 `InvalidOid` (通常是0)？
	 * 有一种常见的情况是，使用 pg_restore（比如运维在做全库从备份中恢复）的时候，
	 * Extension 是先装了，但是其下挂载的表还在重建排队中，此时可能取不到表的 OID。
	 * 与其在这里爆绿（PANIC/FATAL）导致恢复失败，不如留个 warning 并和平退让。
	 */
	if (cachedInfo.sr_plans_oid == InvalidOid ||
		cachedInfo.sr_index_oid == InvalidOid) {
		elog(WARNING, "sr_plan extension installed incorrectly. Do nothing. It's ok in pg_restore.");
		return false;
	}

	/*
	 * 知识点：动态查找 C 函数的 Oid
	 *
	 * 【我们要做什么？】
	 * 我们的插件要求用户在 SQL 里写这样一个函数：`SELECT * FROM t WHERE id = _p(100)`。
	 * 但这是人看得懂的，在 C 代码深层，当我们要去抓这棵语法树里的 "_p" 时，我们不能用
	 * `if (name == "_p")`，全是通过比较 Oid：`if (funcid == 12345)`，因为整数比较最快。
	 *
	 * 【这几行代码具体干了啥？】
	 * 1. `get_namespace_name`：
	 *    先看看本插件装在哪个“户口本”下（比如一般装在 "public" 或 "sr_plan" schema里）。
	 *
	 * 2. `list_make2(...)`：把户口本名字和人的名字拼成一个 C 语言列表（List）。
	 *    它相当于在构造一个路径：`[ "public", "_p" ]`，告诉系统：“我要找 public 这个小区里的 _p 这个人”。
	 *
	 * 3. `LookupFuncName(...)`：拿着上面打包好的路径去找警察局（系统缓存）查户口。
	 *    - 参数 1：上面构造好的名字路径 `[ "public", "_p" ]`
	 *    - 参数 2：1（表示这是一个带 1 个参数的函数）
	 *    - 参数 3：args（之前数组里定义的 ANYELEMENTOID，指明接收啥参数都行）
	 *    - 参数 4：true（如果找不到，别让数据库崩溃报错，悄悄返回一个空值 InvalidOid 给我自己处理）
	 *    最终警察局返回一个全局唯一的数字（如 16409），把它存到 `cachedInfo.fake_func` 里，以后只认这个号！
	 */
	schema_name = get_namespace_name(cachedInfo.schema_oid);
	func_name_list = list_make2(makeString(schema_name), makeString("_p"));
	cachedInfo.fake_func = LookupFuncName(func_name_list, 1, args, true);

	/* 知识点：良好内存释放习惯
	 * 查完立马把链表和提取出来的字符串指针释放还给内存上下文，防止会话长时间不退出导致的堆积性内存泄露。
	 */
	list_free(func_name_list);
	pfree(schema_name);

	/*
	 * 容错检查：如果连本命函数 _p() 都搜不到，说明用户刚才 CREATE EXTENSION 根本没成功或者被破坏了，
	 * 这里也是为了避免 C 导致数据库崩溃重启而做的静默降级处理，打印一句警告就当作插件这会不凑效放行罢了。
	 */
	if (cachedInfo.fake_func == InvalidOid) {
		elog(WARNING, "sr_plan extension installed incorrectly");
		return false;
	}

	/*
	 * 注册一个 Relcache 失效的回调函数 `sr_plan_relcache_hook`。
	 * 这保证了如果用户通过 DROP EXTENSION 或其他方式导致核心表结构发生改变，
	 * 会回调并清空 OID 的缓存，确保我们在下一次请求时能够重新初始化。
	 */
	if (relcache_callback_needed) {
		CacheRegisterRelcacheCallback(sr_plan_relcache_hook, PointerGetDatum(NULL));
		relcache_callback_needed = false;
	}
	return true;
}

/*
 * Check if 'stmt' is ALTER EXTENSION sr_plan
 */
static bool
is_alter_extension_cmd(Node* stmt)
{
	if (!stmt)
		return false;

	if (!IsA(stmt, AlterExtensionStmt))
		return false;

	if (pg_strcasecmp(((AlterExtensionStmt*)stmt)->extname, "sr_plan") == 0)
		return true;

	return false;
}

static bool
is_drop_extension_stmt(Node* stmt)
{
	char* objname;
	DropStmt* ds = (DropStmt*)stmt;

	if (!stmt)
		return false;

	if (!IsA(stmt, DropStmt))
		return false;

#if PG_VERSION_NUM < 100000
	objname = strVal(linitial(linitial(ds->objects)));
#else
	objname = strVal(linitial(ds->objects));
#endif

	if (ds->removeType == OBJECT_EXTENSION &&
		pg_strcasecmp(objname, "sr_plan") == 0)
		return true;

	return false;
}

static void
sr_plan_relcache_hook(Datum arg, Oid relid)
{
	if (relid == InvalidOid || (relid == cachedInfo.sr_plans_oid))
		invalidate_oids();
}

/*
 * TODO: maybe support for EXPLAIN (cached 1)
static void
check_for_explain_cached(ExplainStmt *stmt)
{
	List		*reslist;
	ListCell	*lc;

	if (!IsA(stmt, ExplainStmt))
		return;

	reslist = NIL;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "cached") == 0 &&
				strcmp(defGetString(opt), "on") == 0)
			cachedInfo.explain_query = true;
		else
			reslist = lappend(reslist, opt);
	}

	stmt->options = reslist;
}*/

static void
sr_analyze(ParseState* pstate, Query* query)
{
	cachedInfo.query_text = pstate->p_sourcetext;

	cachedInfo.explain_query = false;

	if (query->commandType == CMD_UTILITY) {
		if (IsA(query->utilityStmt, ExplainStmt))
			cachedInfo.explain_query = true;

		/* ... ALTER EXTENSION sr_plan */
		if (is_alter_extension_cmd(query->utilityStmt))
			invalidate_oids();

		/* ... DROP EXTENSION sr_plan */
		if (is_drop_extension_stmt(query->utilityStmt)) {
			invalidate_oids();
			cachedInfo.enabled = false;
			elog(NOTICE, "sr_plan was disabled");
		}
	}
	if (srplan_post_parse_analyze_hook_next)
		srplan_post_parse_analyze_hook_next(pstate, query);
}

/*
 * 获取 `sr_plan` 扩展安装所在的 schema（命名空间）的 OID。
 * 如果找不到或当前无法获取，则返回 InvalidOid。
 */
static Oid
get_sr_plan_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_schema;
	LOCKMODE heap_lock = AccessShareLock; // 申请共享锁防读取时被修改

	/*
	 * 检查当前是否在事务状态内。如果在事务外（比如某些系统启动阶段），
	 * 读取系统表（这里指元数据目录 pg_extension）是不允许的，安全的做法是直接返回。
	 */
	if (!IsTransactionState())
		return InvalidOid;

	/* 从 Postgres 的内建函数获取 sr_plan 这个扩展本身的 OID */
	ext_schema = get_extension_oid("sr_plan", true);
	if (ext_schema == InvalidOid)
		return InvalidOid; /* exit if sr_plan does not exist */

	/*
	 * 初始化用于扫描 pg_extension 的过滤键 (ScanKey)。
	 * 匹配条件为：查找 OID 等于刚刚获得的扩展 ext_schema OID 的那行数据。
	 * 注意根据不同 PG 版本选取的字段编号有变化。
	 */
#if PG_VERSION_NUM >= 120000
	ScanKeyInit(&entry[0],
		Anum_pg_extension_oid,
		BTEqualStrategyNumber, F_OIDEQ,
		ObjectIdGetDatum(ext_schema));
#else
	ScanKeyInit(&entry[0],
		ObjectIdAttributeNumber,
		BTEqualStrategyNumber, F_OIDEQ,
		ObjectIdGetDatum(ext_schema));
#endif

	/* 打开系统表 pg_extension 准备扫描 */
#if PG_VERSION_NUM >= 130000
	rel = table_open(ExtensionRelationId, heap_lock);
#else
	rel = heap_open(ExtensionRelationId, heap_lock);
#endif

	/*
	 * 开启 systable 的索引扫描 (ExtensionOidIndexId)。
	 * 这是底层的表扫描接口，比执行 SQL 查找更快。
	 */
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
		NULL, 1, entry);

	/* 取出匹配的一条元组(Row) */
	tuple = systable_getnext(scandesc);

	/* 如果取到了有效元组，就将元组转换为 Form_pg_extension 结构并从中获取 extnamespace 字段（即安装的 Schema OID）*/
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension)GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	/* 结束扫描并释放资源 */
	systable_endscan(scandesc);

#if PG_VERSION_NUM >= 130000
	table_close(rel, heap_lock);
#else
	heap_close(rel, heap_lock);
#endif

	return result;
}

/*
 * 根据给定的系统关系表对象的字符串名字 (如 "sr_plans")，在我们设定的特定的 Schema 内，
 * 换取真正用于底层快速驱动内存定位和缓存匹配的实体唯一数字主键标识 (Oid)。
 *
 * 知识点：模式(Schema)限制
 * 为什么不只传名字，偏偏强制要求传 `schema_oid`？
 * 因为在 PG 中，不同的 Schema(比如 public 和定制化的 sr_plan_schema) 里可以存在同名的表！
 * 通过锁定只在插件专属的 Oid 命名空间底下查，就能在任何复杂业务环境下都精确避免重名冲突。
 */
static Oid
sr_get_relname_oid(Oid schema_oid, const char* relname)
{
	/*
	 * 容错及后备初始化处理：
	 * 如果调用方（或者之前的获取环节）没有拿到/弄丢了预解析出来的 Schema，
	 * 这里作为一个底层功能函数再“最后努力尝试帮其重新获取一次”。
	 */
	if (schema_oid == InvalidOid)
		schema_oid = get_sr_plan_schema();

	/* 要是真获取不到本插件运行的基础依托命名空间（代表扩展可能被破坏或未完整安装），撤退打回死账 */
	if (schema_oid == InvalidOid)
		return InvalidOid;

	/*
	 * 调用 PostgreSQL syscache 查询接口:
	 * 把 "关系字符名 + 所属结构空间" 的一对多联合查询请求发出，
	 * PostgreSQL 内部会走它非常高效的 Hash LRU System Cache 返回我们要的最终 Oid 对象名。
	 */
	return get_relname_relid(relname, schema_oid);
}

static void
params_restore_visitor(Plan* plan, void* context)
{
	expression_tree_walker((Node*)plan->qual, sr_query_expr_walker, context);
	expression_tree_walker((Node*)plan->targetlist, sr_query_expr_walker, context);
}

static void
restore_params(void* context, Plan* plan)
{
	plan_tree_visitor(plan, params_restore_visitor, context);
}

/*
 * 知识点：执行计划树的遍历与节点模式匹配 (Visitor Pattern)
 * 在 PostgreSQL 中，执行计划（Plan）是一个在内存中的树状结构（Plan Tree）。
 * 这里实现了一个访问者（Visitor）回调函数，它作为遍历过程中针对每个节点的处理逻辑，
 * 专门用于找出计划里的所有类型的“索引扫描”，并把关联的索引 OID 收集下来。
 */
static void
collect_indexid_visitor(Plan* plan, void* context)
{
	/*
	 * context（上下文）是一个用户自定义的指针，在这里强转回我们在最初调用时传入的链表结构。
	 * 借助这个指针，所有递归/遍历收集到的索引 OID 都能统一汇总在这个结构中。
	 */
	struct IndexIds* index_ids = context;

	/* 边界安全检查：如果遇到计划树分支的尽头为空，直接退出 */
	if (plan == NULL)
		return;

	/*
	 * 知识点：IsA 宏与面向对象模拟
	 * PG 内核虽然是 C 语言写的，但模拟了面向对象的继承机制。所有的节点类型都继承自基类 `Node` 里的一个 tag。
	 * IsA 宏用于安全的运行时类型推断（RTTI）。如果当前计划节点属于普通的“索引扫描 (IndexScan)”，执行进去：
	 */
	if (IsA(plan, IndexScan)) {
		/* 将基类指针 (Plan*) 强转为派生类的指针结构 (IndexScan*) */
		IndexScan* scan = (IndexScan*)plan;

		/*
		 * 知识点：PG 内置的 List 链表操作
		 * lappend_oid 是 PG 为了链表追加数据提供的功能函数。
		 * 它将提取出该扫描节点挂载的索引实体 OID (scan->indexid)，并接在我们统一的链表 ids 尾部。
		 */
		index_ids->ids = lappend_oid(index_ids->ids, scan->indexid);
	}

	/*
	 * 同理，匹配“覆盖索引扫描 (Index Only Scan)”
	 * 覆盖索引扫描是指查询所需字段已全部存在于索引内，PG 可以直接返回索引内容而无需二次回表读取堆数据的一种高级扫描方式
	 */
	if (IsA(plan, IndexOnlyScan)) {
		IndexOnlyScan* scan = (IndexOnlyScan*)plan;
		index_ids->ids = lappend_oid(index_ids->ids, scan->indexid);
	}

	/*
	 * 匹配“位图索引扫描 (Bitmap Index Scan)”
	 * 当扫描命中的数据行较多但又达不到使用顺序全表扫描的阈值时，PG 会读取索引在内存中建立一张位图（Bitmap），
	 * 将零散的行指针排序整合后，再去批量的请求磁盘执行读取，借此极大减少磁盘的随机 IO，这里同样要把涉及的索引记录下来
	 */
	if (IsA(plan, BitmapIndexScan)) {
		BitmapIndexScan* scan = (BitmapIndexScan*)plan;
		index_ids->ids = lappend_oid(index_ids->ids, scan->indexid);
	}
}

/*
 * 知识点：驱动函数（遍历启动）
 * 这个函数是对外暴露的接口封装。
 * 之所以要把 `collect_indexid_visitor` 单独拎出来，是因为实际对树状结构的递归算法已经被 PG /插件内部封在了
 * `plan_tree_visitor` 中，我们只需要将整棵大树（plan）及自定义回调规则传进去即可。
 */
static void
collect_indexid(void* context, Plan* plan)
{
	plan_tree_visitor(plan, collect_indexid_visitor, context);
}

/*
 * 在存储执行计划的系统表（sr_plans）中，通过给定的查询哈希值去查找并恢复执行计划。
 *
 * 参数解释：
 * - snapshot: 用于控制可见性的数据库读取快照。
 * - sr_index_rel: 表的关联索引 (存放 Hash 的索引)。
 * - sr_plans_heap: 存放计划和数据的物理数据表 (Heap)。
 * - key: 已经通过 ScanKeyInit 封装好的查找键（里面包着算好的 query_hash）。
 * - context: 包含“真实”参数列表的上下文对象，如果是通过钩子触发这里将被用来恢复参数。
 * - index: 查找模式指定标志。当传入 0 时表示仅仅查“enable=true”的生效计划；若非 0 则用于特定的展示功能强制提取某一条。
 * - queryString: 如果不为空，则顺便把数据库表里存着的该计划原始 SQL 字符串文本拷贝给这根指针返回。
 */
static PlannedStmt*
lookup_plan_by_query_hash(Snapshot snapshot, Relation sr_index_rel,
	Relation sr_plans_heap, ScanKey key, void* context, int index, char** queryString)
{
	int				counter = 0;
	PlannedStmt* pl_stmt = NULL;
	HeapTuple		htup;
	IndexScanDesc	query_index_scan;
#if PG_VERSION_NUM >= 120000
	/* PG12 及以上版本，引入了 TupleTableSlot(表槽)机制，提升了底层列读取和解析的效率 */
	TupleTableSlot* slot = table_slot_create(sr_plans_heap, NULL);
#endif

	/*
	 * 初始化索引扫描描述符 (IndexScanDesc)：
	 * 将物理堆表 (sr_plans_heap)、关联的索引 (sr_index_rel) 和前面注册的 MVCC 快照整合到一起。
	 * 其中参数 '1' 表示我们要用到 1 个扫描键（也就是下面要传入的 Hash 条件）。
	 */
	query_index_scan = index_beginscan(sr_plans_heap, sr_index_rel, snapshot, 1, 0);

	/*
	 * 带着参数起跑：
	 * 真正地把前面构造好的 key (即包含了 query_hash 且要求等值匹配的条件) 传入扫描器中。
	 * 相当于启动游标，指针跳过了那些 Hash 不匹配的地方，停在第一条匹配的记录门口，准备获取。
	 */
	index_rescan(query_index_scan, key, 1, NULL, 0);

	/*
	 * 循环：根据索引匹配到的条件，顺藤摸瓜逐条去数据表中抓取完整的物理行记录 (Tuple)。
	 * 在此实现了对 PG 各大版本 API 的向下兼容：
	 *
	 * - PG 12 及之后（上分支）：使用引入的高效 Slot 机制。直接让索引扫描器把匹配到的底层数据流推入预先分配好的 slot 里。
	 * - PG 12 之前（下分支）：传统的做法，每次老老实实通过索引取出数据表里具体行的指针对象 HeapTuple。
	 *
	 * ForwardScanDirection 意味着匹配到了多条就按顺序往后正向扫描。
	 */
#if PG_VERSION_NUM >= 120000
	while (index_getnext_slot(query_index_scan, ForwardScanDirection, slot))
#else
	while ((htup = index_getnext(query_index_scan, ForwardScanDirection)) != NULL)
#endif
	{
		Datum		search_values[Anum_sr_attcount];
		bool		search_nulls[Anum_sr_attcount];
#if PG_VERSION_NUM >= 120000
		bool		shouldFree;

		htup = ExecFetchSlotHeapTuple(slot, false, &shouldFree);

		/*
		 * 如果是较新的 PG，因为数据现在存在 slot 里，我们需要用 ExecFetchSlotHeapTuple 把数据解析出来变出跟旧版一样的 htup。
		 * 第二个参数 false 代表尽量不要物理拷贝数据（优化性能只穿透取指针），
		 * 此时 shouldFree 被系统标记为 false。下面的 Assert(!shouldFree) 也就是断言我们没有在这个操作里制造额外的内存泄露风险。
		 */
		Assert(!shouldFree);
#endif

		/*
		 * 解构数据行 (Deform Tuple):
		 * 数据库底层存放的行记录（HeapTuple）是极致紧凑的二进制序列化格式（去除了一切多余 Padding，且没有对齐定长）。
		 * 必须通过 heap_deform_tuple，并结合表的元数据字典（rd_att，记录了有几个字段、什么类型、多宽），
		 * 才能把这个整块的面包切成我们可以独立读取的片段。
		 * 提取出来的每一列的原始内容塞进 search_values (Datum 数组)，把是 NULL 的状态塞进 search_nulls 数组。
		 */
		heap_deform_tuple(htup, sr_plans_heap->rd_att, search_values, search_nulls);

		/* Check enabled field or index */
		/*
		 * 根据匹配模式决定是否采用这个计划：
		 * 1) 如果有指定强找某一条记录(index > 0 && index == counter)。多用于展示函数 `show_plan`
		 * 2) 如果是标准的拦截复用计划阶段(index == 0)，仅仅拾取被标记激活启用的执行计划(Anum_sr_enable列 == true)
		 */
		counter++;
		if ((index > 0 && index == counter) ||
			(index == 0 && DatumGetBool(search_values[Anum_sr_enable - 1]))) {

			/* 从数据库 text 列中提取序列化的计划文本字符串, 借助 PostgreSQL 内建的反序列化将其变成 C 结构体的内存指针对象(PlannedStmt) */
			char* out = TextDatumGetCString(DatumGetTextP((search_values[Anum_sr_plan - 1])));
			pl_stmt = stringToNode(out);

			/* 如需原始语句返回，按相同方法剥离文本塞进输出参数 */
			if (queryString)
				*queryString = TextDatumGetCString(
					DatumGetTextP((search_values[Anum_sr_query - 1])));

			/* 核心：如果传入了包含参数的执行上下文，执行参数 “恢复重绑” 操作 */
			if (context)
				execute_for_plantree(pl_stmt, restore_params, context);

			/* 只取第一条成功的计划直接跳出返回，不再继续搜扫。*/
			break;
		}
	}

	/* 善后擦屁股，关闭本轮索引扫描资源 */
	index_endscan(query_index_scan);
#if PG_VERSION_NUM >= 120000
	ExecDropSingleTupleTableSlot(slot);
#endif
	return pl_stmt;
}

/*
 * 核心：计划器钩子(planner_hook)，拦截每次 SELECT 语句的执行计划生成。
 * 它的主要作用是根据查询树(Query)和当前的插件配置(write_mode等)，
 * 决定是生成新的执行计划并保存，还是从 `sr_plans` 表中复用旧的执行计划。
 */
static PlannedStmt*
#if PG_VERSION_NUM >= 130000
sr_planner(Query* parse, const char* query_string, int cursorOptions, ParamListInfo boundParams)
#else
sr_planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
#endif
{
	Datum			query_hash;
	Relation		sr_plans_heap, sr_index_rel;
	HeapTuple		tuple;
	char* plan_text;
	Snapshot		snapshot;
	ScanKeyData		key;
	bool			found;
	Datum			plan_hash;
	IndexScanDesc	query_index_scan;
	PlannedStmt* pl_stmt = NULL;
	LOCKMODE		heap_lock = AccessShareLock; // 默认使用共享锁
	struct QueryParamsContext qp_context = { true, NULL };
#if PG_VERSION_NUM >= 120000
	TupleTableSlot* slot;
#endif
	static int		level = 0; // 递归层级记录（处理嵌套查询）

	level++;

	/* 按版本定义对标准计划器(或前一个注册的钩子)的调用包裹宏 */
#if PG_VERSION_NUM >= 130000
#define call_standard_planner() \
	(srplan_planner_hook_next ? \
		srplan_planner_hook_next(parse, query_string, cursorOptions, boundParams) : \
		standard_planner(parse, query_string, cursorOptions, boundParams))
#else
#define call_standard_planner() \
	(srplan_planner_hook_next ? \
		srplan_planner_hook_next(parse, cursorOptions, boundParams) : \
		standard_planner(parse, cursorOptions, boundParams))
#endif

	/*
	 * 拦截过滤：
	 * 只处理 SELECT 查询；如果插件未开启或者当前是 EXPLAIN 查询，
	 * 直接调用标准的 PostgreSQL planner 返回，不进行缓存操作。
	 */
	if (parse->commandType != CMD_SELECT || !cachedInfo.enabled
		|| cachedInfo.explain_query) {
		pl_stmt = call_standard_planner();
		level--;
		return pl_stmt;
	}

	/*
	 * OID 懒加载初始化：
	 * 如果插件缓存中的 schema OID 还没就绪，尝试初始化 `sr_plan` 的表和库。
	 * 初始化失败则退回到使用标准的 planner。
	 */
	if (cachedInfo.schema_oid == InvalidOid) {
		if (!init_sr_plan()) {
			/* Just call standard_planner() if schema doesn't exist. */
			pl_stmt = call_standard_planner();
			level--;
			return pl_stmt;
		}
	}
	if (cachedInfo.schema_oid == InvalidOid || cachedInfo.sr_plans_oid == InvalidOid) {
		/* Just call standard_planner() if schema doesn't exist. */
		pl_stmt = call_standard_planner();
		level--;
		return pl_stmt;
	}

	/*
	 * 准备查找阶段：
	 * 1. 遍历查询树，使用 qp_context 收集所有涉及的 _p 伪函数的参数，便于最后恢复
	 * 2. 剥除常量后，计算一个查询的唯一个 hash 值(query_hash)
	 * 3. 用 hash 值去构建后续扫描 `sr_plans` 索引的 ScanKey
	 */
	sr_query_walker((Query*)parse, &qp_context);
	query_hash = get_query_hash(parse);

	/*
	 * 初始化用于索引扫描的查询键 (ScanKey)。
	 * 这相当于在 PostgreSQL 内部底层的存储引擎里构造了一个查询条件：
	 * AND 当前索引的第 1 列 = {此时计算出的 query_hash}
	 *
	 * 参数详解：
	 * - &key: 接收初始化结果的扫描键结构体指针。
	 * - 1: 指定这是对应索引的第 1 列（在我们的表设计中就是 query_hash 字段）。
	 * - BTEqualStrategyNumber: 指定扫描策略为 B-Tree 的“完全等于 (=)”策略。
	 * - F_INT4EQ: 指定比较时使用的底层函数 OID 是整数相等 (int4eq)，因为生成的 Hash 值被存为 32 位整数 (Datum)。
	 * - query_hash: 前一步刚刚计算出来的具体哈希值。
	 */
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, query_hash);

	/*
	 * 使用共享锁 (AccessShareLock) 打开存放查询计划的表和关联它的索引。
	 * 在查找（读）计划的阶段，加上共享锁可以防止其他后端的并发会话或者后台进程
	 * 将这张表 Drop 掉或更改表结构（ALTER TABLE 等排它操作）。
	 * 但共享锁不会阻塞别人读取或往这个表里插入新的计划。
	 */
	heap_lock = AccessShareLock;

	/*
	 * 根据 PG 大版本的不同，使用不同的底层 API 来打开堆表。
	 * PG13 之前叫 heap_open，PG13 开始存储引擎发生了大重构（引入了表级方法的抽象），更名为 table_open。
	 * 获取的指针 sr_plans_heap 将作为读取数据的直接句柄。
	 */
#if PG_VERSION_NUM >= 130000
	sr_plans_heap = table_open(cachedInfo.sr_plans_oid, heap_lock);
#else
	sr_plans_heap = heap_open(cachedInfo.sr_plans_oid, heap_lock);
#endif

	/* 同样，通过之前保存的索引记录的 OID (sr_index_oid)，打开该表对应的索引树 */
	sr_index_rel = index_open(cachedInfo.sr_index_oid, heap_lock);

	/*
	 * 准备进入恢复阶段：
	 * 前面调用 sr_query_walker 时 (collect=true) 已经完成了当前查询的参数“收集”。
	 * 此时将标志位设为 false（进入恢复模式）。如果稍后在缓存表里找到了匹配的执行计划，
	 * 我们还需要遍历反序列化出来的旧计划树，利用这个上下文把刚才收集到的真实参数按位置逐一塞回去。
	 */
	qp_context.collect = false;

	/*
	 * 获取访问表数据的可见性快照 (MVCC Snapshot)：
	 * PostgreSQL 读取任何系统或用户表都需要快照来判断数据版本（Tuple）的可见性。
	 * GetLatestSnapshot() 获取当前数据库最新已提交数据的可见状态。
	 * 使用 RegisterSnapshot 把它注册在内存里（增加引用计数），防止读取中途快照被销毁或被 Vacuum 垃圾回收清理掉。
	 * （这是非常规范的 PG 插件读表起手式）
	 */
	snapshot = RegisterSnapshot(GetLatestSnapshot());

	/*
	 * 核心读取分支：
	 * 从系统表中查找相同 query_hash 对应的执行计划 (前提是 enable 设为 true)
	 * 如果查到了，就直接反序列化使用旧的计划作为结果，跳过所有的算子代价估算！并跳出进入 cleanup 清理。
	 */
	pl_stmt = lookup_plan_by_query_hash(snapshot, sr_index_rel, sr_plans_heap,
		&key, &qp_context, 0, NULL);
	if (pl_stmt != NULL) {
		level--;
		if (cachedInfo.log_usage > 0)
			elog(cachedInfo.log_usage, "sr_plan: cached plan was used for query: %s", cachedInfo.query_text);

		goto cleanup;
	}

	/*
	 * 如果在上面的共享锁读取阶段，我们没能从 sr_plans 系统表中找到当前 SQL 的可用缓存计划
	 * （说明它要么是第一次遇到，要么虽然存进去了但是人肉管理员没有把它的 enable 字段设为 true）。
	 * 那就判断当前系统是否开启了「收集学习模式」(Write Mode)，并且只针对外层的主查询进行学习（拦截 level > 1 防止嵌套无限生成）。
	 * 如果没开学习模式，或者条件不符，那就原样退回给 PG 官方原版的计划器 (call_standard_planner) 当作无事发生，然后跳去结算。
	 */
	if (!cachedInfo.write_mode || level > 1) {
		/* quick way out if not in write mode */
		/* 让常规引擎照常消耗算力和时间去算出执行树路线 */
		pl_stmt = call_standard_planner();
		level--;
		goto cleanup;
	}

	/* ======== 以下开启保存(记录)新计划的逻辑 (Write Mode 触发) ======== */

	/*
	 * ======== 锁升级准备阶段 ========
	 * 因为代码能走到这里，说明 `write_mode` 开启了，且我们决定要把系统刚算出的全新计划写入到存计划的字典表里去了。
	 * 但在这之前我们需要清理战场：
	 * 1. 取消刚才为了在表中“只读查询”而增加引用的内存快照 (UnregisterSnapshot)。
	 * 2. 依次用刚才为了读而持有的共享级别(AccessShareLock)锁关闭字典表 (`sr_plans`) 和索引 (`sr_index_rel`)。
	 * 注意：因为我们马上要变成具有独占破坏性的写入或锁操作，PG 规范要求必须先释放掉低级别的锁（或至少不带冲突的清理），才能重新干净地用排它锁重新获取连接。
	 */
	UnregisterSnapshot(snapshot);
	index_close(sr_index_rel, heap_lock);

	/* 根据大版本号分别调用堆表关闭函数释放资源 */
#if PG_VERSION_NUM >= 130000
	table_close(sr_plans_heap, heap_lock);
#else
	heap_close(sr_plans_heap, heap_lock);
#endif

	/*
	 * ======== 锁升级：排他锁 =======
	 * 前面刚刚释放完读锁，这里开始申请最高级别的访问排他锁（AccessExclusiveLock）。
	 * 这个锁保证了在我们写库查重的这一瞬间，没有任何其它的连接能够删表更别提改结构。
	 * 从而为我们稳当地进行 `INSERT` 新查询记录保驾护航。
	 */
	heap_lock = AccessExclusiveLock;
#if PG_VERSION_NUM >= 130000
	sr_plans_heap = table_open(cachedInfo.sr_plans_oid, heap_lock);
#else
	sr_plans_heap = heap_open(cachedInfo.sr_plans_oid, heap_lock);
#endif
	sr_index_rel = index_open(cachedInfo.sr_index_oid, heap_lock);

	/*
	 * ！！！双重检查锁定 (Double-Checked Locking) 防并发雪崩！！！
	 * 我们刚刚在前面的代码经历过了“无计划 -> 释放读锁 -> 拿到排查写锁”这个时间差。
	 * 在这短短的甚至几微秒的“无锁空窗期”里，如果在高并发环境下（上千个请求涌入同一个 SQL），
	 * 很可能其他拿着锁的 Session 已经眼疾手快地帮咱们把这条一模一样的新计划算完且 `INSERT` 进字典表而且顺带着连 enable 都拉改成 true 了。
	 * 此时我们既然重新拿到了排他锁获取了绝对控制权，就要严谨地再向表里注册一个新的此刻鲜活的快照，拿去重新 lookup 取出执行一次。
	 * 要是这次奇迹般地查到了，说明别人替咱存好了直接用，这叫“趁热捡漏避免全表塞满大量重复 Hash”；
	 * 不然再往下走，就开始真的自己硬上算代价下表写硬盘了。
	 */
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	pl_stmt = lookup_plan_by_query_hash(snapshot, sr_index_rel, sr_plans_heap,
		&key, &qp_context, 0, NULL);

	/* 要是被其他并发线程抢先生成好了（Double-check 生效），捡到现成的就顺着 cleanup 返回直接用 */
	if (pl_stmt != NULL) {
		level--;
		goto cleanup;
	}

	/* 既然库里没有当前的计划，就调用标准计划器计算出一个新计划 */
	pl_stmt = call_standard_planner();
	level--;

	/*
	 * 将标准计划器刚刚新鲜生成的、极为复杂的内部嵌套 C 结构体（PlannedStmt 指针树），
	 * 转化（序列化）为能够落地到硬盘的文本表示形式（如同 JSON 格式化）。
	 * 并为其生成一个内容摘要哈希（plan_hash），以便后续用来做排重与绑定。
	 */
	plan_text = nodeToString(pl_stmt);
	plan_hash = hash_any((unsigned char*)plan_text, strlen(plan_text));

	/*
	 * 排查计划字典内是否已经存在完全相同的物理计划了。
	 * 为什么要这么做？因为同一个查询（query_hash 完全一样）完全可能生成千差万别的物理计划（plan_hash）：
	 *
	 * 【例子 1：数据倾斜导致传入参数不同，计划器生成的最佳方案不同】
	 *   我们执行: SELECT * FROM orders WHERE status = $1;
	 *   - A用户传入 $1 = '完成' (占全表99%数据)：计划器觉得全表扫描(SeqScan)最快 -> 算出 plan_hash = 1111
	 *   - B用户传入 $1 = '退款' (占全表1%数据) ：计划器觉得走索引(IndexScan)最快 -> 算出 plan_hash = 2222
	 *   由于我们预处理时把参数都抹成了占位符(常量0)，所以两者的 query_hash 是相同的，都会挂在这个 query_hash 之下。
	 *
	 * 【例子 2：数据量增长或配置变更】
	 *   - 上个月表里有 100 行记录，调用标准计划器生成了 “全表扫描” 计划。	 *   - 这个月表里有 1000 万行记录，再执行这句 SQL 时，调用标准计划器就算出了 “索引扫描” 计划。
	 *
	 * 为了防止 `sr_plans` 字典表里随着时间推移，塞满了几万条一模一样的“全表扫描历史记录”，
	 * 我们需要遍历当前 query_hash 拥有的所有旧计划。如果发现我们要录入的新计划内容 (plan_hash)
	 * 已经在库里存过一次了，直接打上 found = true 标记，本次不再执行 INSERT！
	 */
	query_index_scan = index_beginscan(sr_plans_heap, sr_index_rel,
		snapshot, 1, 0);
	index_rescan(query_index_scan, &key, 1, NULL, 0);
#if PG_VERSION_NUM >= 120000
	slot = table_slot_create(sr_plans_heap, NULL);
#endif

	found = false;
	/*
	 * 开始循环抓取匹配该 SQL (query_hash) 的每一条历史计划记录。
	 * 通过 for 循环，我们会一条条比对当前已存在数据库里的物理计划。
	 */
	for (;;) {
		HeapTuple	htup;
		Datum		search_values[Anum_sr_attcount];
		bool		search_nulls[Anum_sr_attcount];
#if PG_VERSION_NUM >= 120000
		bool		shouldFree;

		/*
		 * 从索引上不断往后正向查找 (ForwardScanDirection)，
		 * 找到属于同一个 query_hash 的堆表元组，并插入到高级版本 PG 专属的 slot 中。
		 * 如果找不到更多的行了，说明历史计划遍历完毕，break 退出退出循环。
		 */
		if (!index_getnext_slot(query_index_scan, ForwardScanDirection, slot))
			break;

		/*
		 * 提取具体的 HeapTuple 数据，参数 false 表示不深度拷贝以节省性能，
		 * 因此 shouldFree 应该为 false。
		 */
		htup = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
		Assert(!shouldFree); /* 确保没有发生意料之外的内存分配 */
#else
		/*
		 * PG 12 之前的版本，直接通过索引去拿数据行所属块内的 TID (Tuple ID)。
		 * 没拿到 TID 就代表所有同 query_hash 的记录已经全部查完。
		 */
		ItemPointer tid = index_getnext_tid(query_index_scan, ForwardScanDirection);
		if (tid == NULL)
			break;

		/* 根据拿到的 TID 取出实际存在堆表上的那一整行数据 (HeapTuple) */
		htup = index_fetch_heap(query_index_scan);
		if (htup == NULL)
			break;
#endif
		/*
		 * 通过 heap_deform_tuple，结合字典表 sr_plans 的元数据 (rd_att)，
		 * 将底层紧密排列的系统级数据行给“解剖”开。
		 * 拆解出来的每一个列的数据，按顺序填充到 search_values 数组中，
		 * 并把是否为空标记记录在 search_nulls 中。
		 */
		heap_deform_tuple(htup, sr_plans_heap->rd_att,
			search_values, search_nulls);

		/*
		 * 核心比对逻辑：
		 * 从刚才切片好的数组中抽出旧计划的 plan_hash (索引是 Anum_sr_plan_hash - 1)。
		 * 将这个过去存下来的执行树哈希值，与刚才标准计划器生成的全新 plan_hash 进行比对。
		 * 如果两侧完全相等，说明我们算出来的新计划树，其实之前已经被记录在案了！
		 */
		if (DatumGetInt32(search_values[Anum_sr_plan_hash - 1]) == DatumGetInt32(plan_hash)) {
			/* 发现了完全相同（冗余）的计划方案，打上已存在标签，避免后面的无意义 INSERT 落盘动作 */
			found = true;
			break; /* 立刻终止循环，节约算力 */
		}
	}

	/* 数据库操作的基本素养：清理案发现场，关闭并释放刚才用于扫描索引的结构资源 */
	index_endscan(query_index_scan);
#if PG_VERSION_NUM >= 120000
	/* 同样清理刚才专门为高版本 PG 开辟的 TupleTableSlot 占用的内存空间 */
	ExecDropSingleTupleTableSlot(slot);
#endif

	/* found == false 说明这是一套库里没有登记过的全新计划，我们将正式执行 INSERT，把新计划保存到数据库表中！ */
	if (!found) {
		/* 用于收集查询计划中包含的全部索引 OID 的链表结构 */
		struct IndexIds	index_ids = { NIL };

		/* relation OID 和 index relation OID 对应的索引的关联句柄 */
		Relation	reloids_index_rel;
		Relation	index_reloids_index_rel;

		/* 用于将相关的表 OID 和 索引 OID 转换为 PostgreSQL 内置的数组类型存储 */
		ArrayType* reloids = NULL;
		ArrayType* index_reloids = NULL;

		/*
		 * values 数组存储即将插入到表中的每一列的真实数据内容（数组元素为 Datum 类型）
		 *
		 * Datum: 它是 PostgreSQL 源码中极其重要的基础类型。
		 * 你可以把它理解为一个“万能指针”或“万能数据块”。如果数据小（比如 int, bool），
		 * 就直接按值存在 Datum 里；如果数据大（比如 text, 数组），Datum 就存一个指向该数据内存的指针。
		 *
		 * 代码中的 Datum values[Anum_sr_attcount]; 就是准备用来存放表里每一列实际数据的数组。
		 */
		Datum		values[Anum_sr_attcount];

		/*
		 * nulls 数组存储即将插入到表中的每一列是否为空值（对应 NULL 标志）
		 *
		 * 代码中的 bool nulls[Anum_sr_attcount]; 就是准备用来存放表里每一列是否为空的数组。
		 * 数据库的列可以是 NULL，但这在 C 语言的普通变量里很难完美表达。
		 * 因此 PG 使用一个并行的布尔数组 bool nulls[Anum_sr_attcount];。
		 * 如果 nulls[i] == true，说明这一列是空值，系统会忽略对应的 values[i]。
		 */
		bool		nulls[Anum_sr_attcount];

		/*
		 * 提取原计划中涉及的实表数量 (即 plan_stmt->relationOids 集合的长度)
		 *
		 * OID：在 PostgreSQL 里，所有的表、视图、索引、甚至是数据类型，在底层都有一个全局唯一的数字 ID 叫做 OID。
		 * 代码中的 pl_stmt->relationOids 就是这个查询计划里涉及到的所有表的 OID 集合。
		 */
		int			reloids_len = list_length(pl_stmt->relationOids);

		/*
		 * 打开 sr_plan 依赖的辅助索引（相关表的 OID 索引以及相关索引的 OID 索引），并加上与堆表一致的锁（heap_lock）
		 *
		 * Relation: 当底层想去读写一张表或索引时，不能只拿 OID，需要调用 index_open() 将它加载到内存中。
		 * 加载出来的巨大内存结构体叫 Relation，里面包含着这张表/索引的全部元数据（有多少列、存在磁盘哪个文件等等）。
		 */
		reloids_index_rel = index_open(cachedInfo.reloids_index_oid, heap_lock);
		index_reloids_index_rel = index_open(cachedInfo.index_reloids_index_oid, heap_lock);

		/* 初始化 nulls 数组，默认全部设置为非空 (0/false) */
		MemSet(nulls, 0, sizeof(nulls));

		/* 构建需要插入的各个字段：包含查询的 hash 值、查询 ID、计划内容的 hash、以及查询原始文本和生成的执行计划文本等 */
		values[Anum_sr_query_hash - 1] = query_hash;
		values[Anum_sr_query_id - 1] = Int64GetDatum(parse->queryId);
		values[Anum_sr_plan_hash - 1] = plan_hash;
		values[Anum_sr_query - 1] = CStringGetTextDatum(cachedInfo.query_text);
		values[Anum_sr_plan - 1] = CStringGetTextDatum(plan_text);
		values[Anum_sr_enable - 1] = BoolGetDatum(false); /* 新收集的计划默认状态为不启用，需要管理员后续确认验证后手工开启 */

		/* 依赖的表 OID 数组 和 索引 OID 数组初始化为 0，后面根据实际情况填充 */
		values[Anum_sr_reloids - 1] = (Datum)0;
		values[Anum_sr_index_reloids - 1] = (Datum)0;

		/* 如果该查询计划涉及了实体表（reloids_len > 0），则收集全部的表 OID 并构造成 PostgreSQL 的内置 Oid 数组 */
		if (reloids_len) {
			int			pos;
			ListCell* lc;

			/*
			 * 分配足够存储全部表 OID 的连续内存数组
			 * 我们在 SQL 里经常写 ARRAY[1, 2, 3]。如果要在 C 层面生成这样一个数组存入表里该怎么做呢？
			 *
			 * 代码先用 palloc (PG 专用的 malloc 内存分配) 开辟了一段连续的 C 数组 Datum* reloids_arr。
			 * 循环收集好 OID 后，调用 construct_array() 函数。这个函数会在内存中额外打包一层结构，
			 * 带上“数组维度”、“每个元素的类型（OIDOID）”等元信息，
			 * 这才是 PostgreSQL 能够识别并存入磁盘的真实数组结构（ArrayType）。
			 */
			Datum* reloids_arr = palloc(sizeof(Datum) * reloids_len);

			pos = 0;
			/* 遍历执行计划中的关系列表（relationOids），逐个将表 OID 存入 reloids_arr 数组 */
			foreach(lc, pl_stmt->relationOids)
			{
				reloids_arr[pos] = ObjectIdGetDatum(lfirst_oid(lc));
				pos++;
			}

			/* 知识点：ArrayType 转换
			 * construct_array 负责将 C 语言中的数据数组（这里是 reloids_arr，每个元素都是 Datum 类型）
			 * 打包成 PostgreSQL 内部认可的并能持久化到磁盘的内置 ArrayType 数组结构。
			 * 提供：数组维度大小、元素的基础类型 OID(也就是本例的 OIDOID)、元素的尺寸长度等等。
			 */
			reloids = construct_array(reloids_arr, reloids_len, OIDOID, sizeof(Oid), true, 'i');
			/* 填充到目标值数组中，准备持久化 */
			values[Anum_sr_reloids - 1] = PointerGetDatum(reloids);

			/* 转换完毕后释放中间内存，避免泄漏 */
			pfree(reloids_arr);
		}
		/* 如果查询没涉及表（可能只有纯计算等），说明依赖表集为空，将其值置为 NULL */
		else nulls[Anum_sr_reloids - 1] = true;

		/* 同样地，遍历执行计划树（plantree），调用 collect_indexid 回调把所有涉及的索引 OID 收集到 index_ids 中 */
		execute_for_plantree(pl_stmt, collect_indexid, (void*)&index_ids);

		/* 如果有涉及的索引，将链表里的索引 OID 同样转化成 PostgreSQL 的内部数组格式 */
		if (list_length(index_ids.ids)) {
			int len = list_length(index_ids.ids);
			int			pos;
			ListCell* lc;

			/*
			 * 知识点：palloc 与 PG 内存上下文
			 * palloc 是 PostgreSQL 自己封装的内存分配函数，替代了标准 C 的 malloc。
			 * PG 的内存是根据“上下文（Memory Context）”来管理的（比如语句上下文、事务上下文）。
			 * 使用 palloc 分配的内存会在对应的上下文结束时被自动回收，从而极大减轻了 C 语言手动防御内存泄漏的压力。
			 * 这里分配了一个 Datum 数组，大小是 索引数量 * Datum的大小。
			 */
			Datum* ids_arr = palloc(sizeof(Datum) * len);

			pos = 0;
			/*
			 * 知识点：ListCell 与 foreach 宏
			 * PostgreSQL 自己实现了一套功能丰富的双向/单向链表 (List)。
			 * `foreach` 是一个宏，用来优雅地迭代遍历整个链表。lc 就是指向当前链表节点的指针（ListCell）。
			 */
			foreach(lc, index_ids.ids)
			{
				/*
				 * 知识点：宏提取与 Datum 转换
				 * lfirst_oid(lc): 这是一个宏，用于从当前链表节点 lc 中直接提取出存放的 Oid 数据。
				 * ObjectIdGetDatum(...): 这是 PG 提供的类型包装宏。由于我们要把数据填进万能数组 ids_arr，
				 * 而该数组存的是 Datum，因此无论实际是 int 还是 Oid，都要套上一层转为 Datum 兼容格式。
				 */
				ids_arr[pos] = ObjectIdGetDatum(lfirst_oid(lc));
				pos++;
			}
			/* 构造索引 OID 类型数组（知识点同上面的表 OID 数组解析） */
			index_reloids = construct_array(ids_arr, len, OIDOID, sizeof(Oid), true, 'i');

			/*
			 * 把生成的内部 Array 数据结构的指针（依然要包上 PointerGetDatum），
			 * 安置到之前准备好的大 values 数组的对应列位置中。
			 */
			values[Anum_sr_index_reloids - 1] = PointerGetDatum(index_reloids);

			/* 用完马上显式释放（虽然事务结束也会释放，但这里提前释放更节省内存） */
			pfree(ids_arr);
		}
		else {
			/*
			 * 知识点：C 语言数组下标 与 PostgreSQL 列号 (Attribute Number) 的转换
			 * Anum_sr_index_reloids 是插件表定义时自动生成的宏，代表该字段是表里的第几个列。
			 * PostgreSQL 内部约定，表的物理列号是从 1 开始的（1-based）。
			 * 而 C 语言的 values 和 nulls 数组下标是从 0 开始的（0-based）。
			 * 因此向对应列塞值或者标记为空时，永远需要通过 `列宏 - 1` 来对齐正确的内存偏移量。
			 * 这里 nulls[...] = true 意味着这行数据的这个字段是一个纯粹的 NULL 值（因为本次查询没用到任何索引）。
			 */
			nulls[Anum_sr_index_reloids - 1] = true;
		}

		/* 知识点：HeapTuple (物理元组)
		 * heap_form_tuple() 函数相当于一个压缩打包机。它根据表的列定义信息 (sr_plans_heap->rd_att)，
		 * 结合刚才准备好的真实数据 values 数组，和空值判定 nulls 数组，将它们紧凑地序列化并压缩成连续的二进制内存块。
		 * 这样一块格式化的内存数据被称为 HeapTuple（堆表元组），这才是真正能够写入磁盘的数据格式。
		 */
		tuple = heap_form_tuple(sr_plans_heap->rd_att, values, nulls);

		/*
		 * 知识点：底层数据插入与 WAL 机制
		 * simple_heap_insert 负责将装配好的 Tuple 真正写进物理堆表。
		 * “simple” 的原因在于它绕过了 PostgreSQL 上层的解析器、重写器、规划器等全部 SQL 环节，是 O(1) 损耗的数据入库方式。
		 * 但需要注意的是它并不 simple 到连日志都不记：它仍然会安全地遵守并生成 WAL（Write-Ahead Logging，预写式日志），
		 * 因此即使系统崩溃，这个强行塞进去的 Tuple 数据也是可恢复并且跨节点复制安全的，不会破坏数据库的 ACID 特性。
		 */
		simple_heap_insert(sr_plans_heap, tuple);

		/* 根据配置是否输出日志信息 */
		if (cachedInfo.log_usage)
			elog(cachedInfo.log_usage, "sr_plan: saved plan for %s", cachedInfo.query_text);

		/* 知识点：底层 C 开发中的索引同步更新
		 * 如果是通过 SQL 执行 INSERT，数据库会自动帮你同步所有索引。但在 C 层调用 simple_heap_insert，索引和堆表(Heap)是分离的。
		 * 我们需要针对表上的每一个索引，手动调用 index_insert_compat 函数（在高版本通常叫 index_insert）。
		 * 该函数会将刚才插入元组的位置信息( &(tuple->t_self) ，也叫 CTID )和当前的键值对登记到 B树或哈希等索引结构中。
		 */
		 /* 更新主查询 hash 对应的主键查询索引 */
		index_insert_compat(sr_index_rel,
			values, nulls,
			&(tuple->t_self),
			sr_plans_heap,
			UNIQUE_CHECK_NO);

		/*
		 * 知识点：针对特定列的局部索引写入
		 * 如果查询真的涉及了表 (reloids 不空)，才需要去更新这个专门为 reloids 列建的独立索引。
		 * 注意看参数：这里没有传完整的 values 数组，而是传了 &values[Anum_sr_reloids - 1]。
		 * 相当于在这单独建立的一个局部的“反向查询字典”中只插入这一单个字段对应的数据结构。
		 *
		 * UNIQUE_CHECK_NO：表示这个更新不需要做“唯一键冲突检查”（因为一个表很可能被多个查询用到），
		 * 节省因为需要阻塞检测唯一性而导致的大量索引页面加锁的性能开销。
		 */
		if (reloids) {
			index_insert_compat(reloids_index_rel,
				&values[Anum_sr_reloids - 1],
				&nulls[Anum_sr_reloids - 1],
				&(tuple->t_self),
				sr_plans_heap,
				UNIQUE_CHECK_NO);
		}

		/*
		 * 同步维护索引数组依赖的专属关联索引
		 *
		 * 知识点：同样的局部单字段索引同步方式。
		 * 如果存在查到的底层索引信息 (index_reloids_len > 0 或直接判断 index_reloids 数组不为空)，
		 * 才会将这组代表所选底层索引信息的 OID 数组及其行指针提交到底层 GIN 等索引树上进行维护更新。
		 */
		if (index_reloids) {
			index_insert_compat(index_reloids_index_rel,
				&values[Anum_sr_index_reloids - 1],
				&nulls[Anum_sr_index_reloids - 1],
				&(tuple->t_self),
				sr_plans_heap,
				UNIQUE_CHECK_NO);
		}

		/*
		 * 知识点：资源清理与锁维持策略
		 * index_close 会关闭对应的 Relation 句柄并释放内存结构，避免服务器在长时间运行中导致 Relation 结构体内存泄漏。
		 *
		 * 注意看第二个传入的参数（锁类型）：我们依然传入了之前的 heap_lock（这里最初加的是写冲突级别的锁或共享锁等）。
		 * 原因：我们在这句代码里“关闭”了对应的操作句柄，但并没有“释放(Release)”锁本身。
		 * 这个锁将被 PostgreSQL 内核继续维持，一直保持到当前整个“事务 (Transaction)”彻底结束(COMMIT 或 ROLLBACK)时，
		 * 再由内核统一释放（所谓两阶段锁协议, 2PL），从而防止在当前事务还未结束、数据尚未定型时，其他并发会话读到“半吊子”的不一致脏数据。
		 */
		index_close(reloids_index_rel, heap_lock);
		index_close(index_reloids_index_rel, heap_lock);

		/*
		 * 知识点：命令可见性推进与 MVCC（CommandCounterIncrement）
		 *
		 * 在 PostgreSQL 的 MVCC (多版本并发控制) 体系下，单个大事务(Transaction) 内部还可以包含许多顺序的小命令。
		 * 数据库通过一个叫做 Command Id （Command Counter，从0开始） 的标识来区别同一个事务里谁先谁后。
		 * 举个例子，如果在普通的 SQL 中你写下：
		 * 1. \`INSERT INTO sr_plans ... \`
		 * 2. \`SELECT * FROM sr_plans ... \`
		 * PG 引擎会在解析完第一句后，自动地将当前事务的命令计数器 +1。当你执行第二句 SELECT 获取数据快照时，
		 * 快照就会将刚刚 +1 之前的那些数据变动评估为“发生在过去的有效且可见修改”。
		 *
		 * 但是！！我们在上面调用的是底层的 C 函数 `simple_heap_insert(...)`，它太过于“直击灵魂”，
		 * 单纯写了磁盘，可完全没理会并推动上层的控制抽象。
		 * 如果就在它的下一行代码我们去触发查找缓存，哪怕数据物理上就在表里，查询代码也会像瞎子一样直接把这行刚插入的数据忽略掉！
		 *
		 * 结论：
		 * 调用 `CommandCounterIncrement()` (“强行拨快本地事务的逻辑时钟一格和刷新内部可见快照指针”)。
		 * 这个动作本质是在向当前的存储引擎注册：“我们当前的一组批量高敏操作（插入表和多树索引）刚刚圆满且原子性的告一段落啦”，
		 * 保证此行下面的 C 代码无论再走哪个普通查询通道，都能“立刻看破生死”，读到这几行新鲜插入进来的元组。
		 */
		CommandCounterIncrement();
	}

cleanup:
	/*
	 * 知识点：快照清理与内存解绑
	 * `UnregisterSnapshot` 用于解除我们在本段函数片头用 `RegisterSnapshot` 注册压栈锁定的那份数据库当前瞬时的数据快照。
	 * 这非常关键：如果注册后不解除，这个快照引用的系统内存游标就会一直残留着，
	 * 会拉长甚至拖垮后续整个 PostgreSQL 后台真空回收机制（VACUUM）对已死亡过期无用旧元组的物理清理能力（产生大量锁膨胀）。
	 */
	UnregisterSnapshot(snapshot);

	/*
	 * 知识点：通用堆表和索引句柄安全清理与 API 版本兼容
	 * 这个地方是对前面 `index_open` 和 `table_open/heap_open` 的一一收尾动作，保持引用计数的平衡。
	 * 这里之所以要再写一个 #if #else，是因为在 PostgreSQL 12 和 13 期间，这部分 API 经历了一次重大的重命名重构。
	 *
	 * PG12 开始，官方将操作表的抽象概念化了（比如表将来不一定非是 Heap，甚至可能是基于列存 zheap 或是网络等新的表存储格式），
	 * 于是在 PG13 把 `heap_close`（堆表关闭） 强制改成了对开发者更统一的抽象：`table_close`（表格关闭）。
	 */
	index_close(sr_index_rel, heap_lock);
#if PG_VERSION_NUM >= 130000
	table_close(sr_plans_heap, heap_lock);
#else
	heap_close(sr_plans_heap, heap_lock);
#endif

	return pl_stmt;
}

/*
 * 遍历查询树 (Query Tree) 的底层路由/入口函数。
 * 它的主要作用是在查询的抽象语法树 (AST) 中寻找包含具体条件表达式的节点，
 * 并交由子处理函数来提取或还原我们特殊注入的伪函数 `_p` 的相关参数。
 */
static bool
sr_query_walker(Query* node, void* context)
{
	/* 递归终止条件防空指针异常 */
	if (node == NULL)
		return false;

	/*
	 * 检查是否有我们需要额外干预的特定节点：
	 * 如果遇到 FromExpr 类型（代表 FROM 表达式，其中包含 WHERE 过滤条件或 JOIN 条件），
	 * 此时代表我们进入了真正的逻辑表达式区域，调用专用的 sr_query_expr_walker 遍历内部细节。
	 */
	if (IsA(node, FromExpr))
		return sr_query_expr_walker((Node*)node, context);

	/*
	 * 对于没有特殊逻辑且确认为通用 Query 树的节点（例如最外层查询，或嵌套子查询的入口），
	 * 调用 PostgreSQL 核心库提供的 query_tree_walker 自动递归展开其各个子结构，
	 * 并将当前的 sr_query_walker 自身作为迭代回调接着向下处理，
	 * 直到找出其内部藏着的 FromExpr 为止。
	 */
	if (IsA(node, Query))
		return query_tree_walker(node, sr_query_walker, context, 0);

	/* false 代表当前节点不阻断遍历过程，即“继续查找整个树” */
	return false;
}

/*
 * 表达式树遍历器 (Expression Tree Walker)。
 * 主要处理具体的表达式节点（例如 WHERE a = _p(2) 中的 _p 伪函数）。
 * 这个函数有两种工作模式，由 qp_context->collect 决定：
 * 1. 收集模式 (collect=true): 找出所有 _p 函数调用，将其中的实际参数值暂存起来，
 *    并把 _p 函数的 funccollid 临时替换为它在 SQL 文本中的位置 (location) 作为后续映射的钥匙。
 * 2. 恢复模式 (collect=false): 在从缓存中取出执行计划准备执行时，
 *    根据之前被 HACK 过的 funccollid 找回对应的参数值并塞回语法树中，以实现参数化查询的直接复用。
 *
 * 例子一：首次执行并保存计划（触发“收集模式”）
 *   SQL: SELECT * FROM users WHERE age = _p(100);
 *   - 找到 _p(100) 节点，将其实际值 Const(100) 保存进参数列表 (qp_context->params)。
 *   - 把 _p() 函数节点的 funccollid 偷偷替换为它在 SQL 文本中的字符位置（假设为 36）。
 *   - 随后系统计算出通用的 hash 并存入 sr_plans 系统表。此时存进去的计划里，保留了 funccollid 为 36 这一特征。
 *
 * 例子二：此后执行复用计划（触发“恢复模式”）
 *   SQL: SELECT * FROM users WHERE age = _p(20);
 *   - 拦截查询，首先在“收集模式”下收集到新参数 Const(20) 以及它的位置（例如 location=36）。
 *   - 从 `sr_plans` 取出旧计划准备复用。此时开启“恢复模式”去遍历从表里拿出来的旧语法树。
 *   - 遇到 _p() 节点，发现它的 funccollid 是当初存进去的 36。拿 36 去刚刚收集的参数列表中精确匹配。
 *   - 匹配成功，将 Const(20) 和原本正确的 funccollid 塞回旧树中。新参数绑定完毕，跳过生成步骤直接执行该树。
 */
static bool
sr_query_expr_walker(Node* node, void* context)
{
	/* 将通用的 void* 上下文转换为我们的参数收集上下文结构体 */
	struct QueryParamsContext* qp_context = context;
	FuncExpr* fexpr = (FuncExpr*)node;

	/* 递归终止条件 */
	if (node == NULL)
		return false;

	/*
	 * 找到了目标：如果当前节点是一个函数调用表达式，
	 * 且该函数的 OID 正好是我们在 init_sr_plan() 中记录的伪函数 `_p`
	 */
	if (IsA(node, FuncExpr) && fexpr->funcid == cachedInfo.fake_func) {
		/* 模式 1：收集参数阶段 (通常在生成计划或计算 hash 前进行) */
		if (qp_context->collect) {
			/* 分配内存保存参数信息 */
			struct QueryParam* param = (struct QueryParam*)palloc(sizeof(struct QueryParam));
			param->location = fexpr->location; // 记录此函数调用在原始 SQL 字符串中的字符位置

			/*
			 * 提取 _p 函数内部实际包裹的参数节点指针。
			 * 根据 PG 版本 API 的不同，从参数列表 (args) 中提取首个子元素的做法有所差异。
			 */
#if PG_VERSION_NUM >= 130000
			param->node = fexpr->args->elements[0].ptr_value;
#else
			param->node = fexpr->args->head->data.ptr_value;
#endif
			param->funccollid = fexpr->funccollid; // 备份原来的 collation OID

			/*
			 * HACK（黑魔法）: 强制把当前函数表达式的 funccollid (排序规则 OID) 覆盖为 location。
			 * 原因是标准的 PG 计划过程（planning）可能由于拷贝、折叠等操作使得 location 丢失，
			 * 我们必须把这个标识符藏在序列化时不会丢失的 funccollid 字段中，作为恢复阶段匹配的唯一凭证。
			 */
			fexpr->funccollid = fexpr->location;

			if (cachedInfo.log_usage)
				elog(cachedInfo.log_usage, "sr_plan: collected parameter on %d", param->location);

			/* 将收集到的参数信息挂载到上下文的 List 中 */
			qp_context->params = lappend(qp_context->params, param);
		}
		else {
			/* 模式 2：恢复参数阶段 (通常在从缓存读取出计划字符串并反序列化后进行) */
			ListCell* lc;

			/* 遍历之前收集到的所有参数 */
			foreach(lc, qp_context->params)
			{
				struct QueryParam* param = lfirst(lc);

				/*
				 * 利用之前收集时的 Hack 手段，使用被替换的 funccollid 去匹配参数信息结构中的 location。
				 * 如果两边能对上，说明这就是对应的那个 _p() 节点。
				 */
				if (param->location == fexpr->funccollid) {
					/* 匹配成功，恢复它原本该有的 funccollid 数据 */
					fexpr->funccollid = param->funccollid;

					/* 将真实的动态参数节点塞回函数表达式，完成了此查询语句的“变量绑定” */
#if PG_VERSION_NUM >= 130000
					fexpr->args->elements[0].ptr_value = param->node;
#else
					fexpr->args->head->data.ptr_value = param->node;
#endif
					if (cachedInfo.log_usage)
						elog(cachedInfo.log_usage, "sr_plan: restored parameter on %d", param->location);

					/* 已经找到并恢复完成，提前跳出本次寻找循环 */
					break;
				}
			}
		}

		/* 处理完毕当前的伪函数节点，不再往该函数内部钻取，返回 false 表示不阻断其他支树的遍历 */
		return false;
	}

	/* 如果不是我们要找的 _p 伪函数，就用 PG 核心的 expression_tree_walker 继续无脑剥下层表达式 */
	return expression_tree_walker(node, sr_query_expr_walker, context);
}

/*
 * 假常量表达式树遍历器 (Fake Constant Expression Tree Walker)。
 * 它的核心任务是找到语法树中所有的 _p() 伪函数调用，
 * 然后无视它里面原本写的是什么参数（不管是 1 还是 100 还是 'abc'），
 * 统统将其替换为一个标准的、硬编码的假常量。
 * 这样做的目的是为了抹平不同参数带来的语法树差异，从而计算出相同的 Query Hash。
 *
 * 举例说明：
 * 假设我们有两条相同的结构但参数不同的 SQL：
 * Query A: SELECT * FROM t WHERE id = _p(100);
 * Query B: SELECT * FROM t WHERE id = _p(999);
 * 如果直接计算 Hash，由于 AST 中保存的常量(100和999)不同，它们的 Hash 必定不同。
 * 经过本函数处理后，无论是 100 还是 999，在为计算 Hash 而拷贝出的语法树中，
 * 都会被强行替换为同一个假常量 `0`(INT4)。
 * 从而使得 Query A 和 Query B 序列化出来的结构长得完全一模一样：
 * `SELECT * FROM t WHERE id = _p(0)`
 * 进而生成相同的 Hash，让 Query B 可以通过 Hash 成功找到并复用 Query A 预先存下的执行计划。
 */
static bool
sr_query_fake_const_expr_walker(Node* node, void* context)
{
	FuncExpr* fexpr = (FuncExpr*)node;

	/* 递归终止条件，防止空指针异常 */
	if (node == NULL)
		return false;

	/* 如果我们找到了目标节点：类型是函数调用，并且函数的 OID 确实是我们的伪函数 _p() */
	if (IsA(node, FuncExpr) && fexpr->funcid == cachedInfo.fake_func) {
		Const* fakeconst;

		/*
		 * 创建一个标准的假常量 (Fake Constant)。
		 * makeConst 是 PG 内部构造常量节点的函数。
		 * 这里的参数代表：构造一个值为 0(Datum) 的 INT4 类型常量(Oid=23)。
		 * 无论原始 _p() 里面包裹的是什么，在这一步拷贝的树里，都会被强行改成这个 INT4 的 0。
		 */
		fakeconst = makeConst(23, -1, 0, 4, (Datum)0, false, true);

		/* 把原函数调用的参数列表丢弃，替换为只包含这个单一假常量的新列表 */
		fexpr->args = list_make1(fakeconst);
	}

	/* 继续使用 PG 内部遍历器无脑向下剥开当前节点的下层子节点，寻找更多可能的 _p() */
	return expression_tree_walker(node, sr_query_fake_const_expr_walker, context);
}

/*
 * 假常量树遍历器 (Fake Constant Walker)。
 * 它是为计算唯一哈希值（get_query_hash）服务的总控路线函数。
 * 作用是递归遍历查询语法树（Query Tree），寻找可能包含条件的节点。
 */
static bool
sr_query_fake_const_walker(Node* node, void* context)
{
	/* 递归防空终止条件 */
	if (node == NULL)
		return false;

	/*
	 * 检查节点是否是我们需要进行特殊处理的类型。
	 * 如果是 FromExpr（FROM 表达式，必然包含了逻辑条件等表达式部分），
	 * 那么将任务转交给表达式层级的 walker (sr_query_fake_const_expr_walker)
	 * 去进行参数的“假常量剥离并替换”工作。
	 */
	if (IsA(node, FromExpr))
		return sr_query_fake_const_expr_walker(node, context);

	/*
	 * 如果不是我们马上要处理的核心逻辑节点（即普通的 Query 节点或者子查询包裹节点），
	 * 就调用 PG 核心库的 query_tree_walker 继续向下拆解语法树，
	 * 直到拆解出 FromExpr 为止。
	 */
	if (IsA(node, Query)) {
		Query* q = (Query*)node;
		return query_tree_walker(q, sr_query_fake_const_walker, context, 0);
	}

	return false;
}

/*
 * 计算并返回当前查询树（Query）的唯一哈希值。
 * 这个哈希值将作为 sr_plans 系统表中查询映射计划的“主关键字”。
 *
 * 核心逻辑：
 * 为了让包含不同参数值的同一类查询（比如 a = _p(1) 和 a = _p(2)）能命中同一个计划，
 * 需要在计算哈希之前对语法树进行标准化（剥除具体的参数值，替换为一个固定常量）。
 */
static Datum
get_query_hash(Query* node)
{
	Datum			result;
	Node* copy;
	MemoryContext	tmpctx, oldctx;
	char* temp;

	/*
	 * 创建一个临时的内存上下文（MemoryContext）。
	 * 因为接下来的深拷贝和节点字符串化操作会分配大量的内存，
	 * 使用临时上下文可以在使用完毕后一次性释放，防止内存泄漏。
	 */
	tmpctx = AllocSetContextCreate(CurrentMemoryContext,
		"temporary context",
		ALLOCSET_DEFAULT_SIZES);

	/* 切换到刚刚创建的临时内存上下文 */
	oldctx = MemoryContextSwitchTo(tmpctx);

	/* 1. 对原始的查询语法树进行深拷贝，以免接下来的替换操作污染到真实的计划执行树 */
	copy = copyObject((Node*)node);

	/*
	 * 2. 核心标准化步骤：遍历这棵拷贝出来的树，
	 * 找到所有的伪函数 _p()，然后把里面包裹的真实参数节点替换成一个伪造的常量节点（假常量）。
	 * 这一步保证了无论 _p 里传的是什么，树的结构在这个层面变得完全一致。
	 */
	sr_query_fake_const_walker(copy, NULL);

	/* 3. 将标准化后的抽象语法树（AST）序列化为一个纯文本字符串 */
	temp = nodeToString(copy);

	/* 4. 对生成的查询文本字符串计算哈希散列值（Datum，相当于 int32） */
	result = hash_any((unsigned char*)temp, strlen(temp));

	/* 切换回之前的内存上下文 */
	MemoryContextSwitchTo(oldctx);

	/* 释放临时上下文及其内部分配的所有内存空间（包含 copy 树和 temp 字符串） */
	MemoryContextDelete(tmpctx);

	return result;
}

static const struct config_enum_entry log_usage_options[] = {
	{"none", 0, true},
	{"debug", DEBUG2, true},
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"log", LOG, false},
	{"info", INFO, true},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{NULL, 0, false}
};

void
_PG_init(void)
{
	/*
	 * 知识点：GUC 参数注册 (自定义配置项)
	 * DefineCustomBoolVariable 等 API 允许我们将自定义的插件变量挂载到 PG 统一的 GUC (Grand Unified Configuration)
	 * 框架中。这意味着用户可以通过在 postgresql.conf 文件里配置、或是通过 SQL 语句（SET sr_plan.write_mode = on）
	 * 甚至 SHOW 命令，来实时、热配置我们插件的各项行为。
	 * 这些配置一旦修改，会即刻被映射到绑定地址（如 &cachedInfo.write_mode）的 C 原生变量中去。
	 */
	DefineCustomBoolVariable("sr_plan.write_mode",
		"Save all plans for all queries.",
		NULL,
		&cachedInfo.write_mode,
		false,
		PGC_SUSET, /* 权限要求：仅允许拥有 Superuser/数据库管理员 权限的用户调整，防被恶意爬取所有结构 */
		0,
		NULL,
		NULL,
		NULL);

	DefineCustomBoolVariable("sr_plan.enabled",
		"Enable sr_plan.",
		NULL,
		&cachedInfo.enabled,
		true, /* 默认开启：一旦加载此库就默认拦截并进入检查复用流程 */
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL);

	DefineCustomEnumVariable("sr_plan.log_usage",
		"Log cached plan usage with specified level",
		NULL,
		&cachedInfo.log_usage,
		0,
		log_usage_options, /* 使用自定义枚举名映射到底层的系统级别枚举 */
		PGC_USERSET,     /* 这是用户可以在会话级随意修改以调试其自身行为的变量 */
		0,
		NULL,
		NULL,
		NULL);

	/*
	 * 知识点：核心黑魔法 -> GUC 钩子劫持截留 (Hook Hooking)
	 * 这个地方展示的是所有的 PostgreSQL C Extension 能进行底层干预的最强手段：指针劫持（类似AOP里的切面代理）。
	 * PG 内核在关键流程节点（例如生成执行计划前）预留了函数指针（比如 planner_hook 等）。
	 *
	 * - 第一步：先保存住原始内核/或是更早前被别的扩展占走的钩子指针。
	 *   如：`srplan_planner_hook_next = planner_hook`
	 * - 第二步：把系统的此钩子直接强行指向成本插件所提供的自定义函数（比如我们写好的 sr_planner拦截函数）。
	 *   如：`planner_hook = &sr_planner`
	 *
	 * 当用户跑 SQL 语句触发计划解析时，PG 会调 `planner()` -> 即掉进我们的 `sr_planner()`。
	 * 如果我们判定该语句被 `sr_plan` 管理了，我们就自己下发计划（不再交还控制权）；
	 * 如果我们判定这是一句不需要特殊管理的普通 SQL，就在我们的拦截器底部去调用刚才事先保存在 `srplan_planner_hook_next`
	 * 中的原底层函数，把执行权重新交棒给 PostgreSQL 原版流程（无缝放行）。
	 */
	srplan_planner_hook_next = planner_hook;
	planner_hook = &sr_planner;

	srplan_post_parse_analyze_hook_next = post_parse_analyze_hook;
	post_parse_analyze_hook = &sr_analyze;
}

void
_PG_fini(void)
{
	/* nothing to do */
}

Datum
do_nothing(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum
_p(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
 *	Construct the result tupledesc for an EXPLAIN
 */
static TupleDesc
make_tupledesc(ExplainState* es)
{
	TupleDesc	tupdesc;
	Oid			result_type;

	/* Check for XML format option */
	switch (es->format) {
	case EXPLAIN_FORMAT_XML:
		result_type = XMLOID;
		break;
	case EXPLAIN_FORMAT_JSON:
		result_type = JSONOID;
		break;
	default:
		result_type = TEXTOID;
	}

	/* Need a tuple descriptor representing a single TEXT or XML column */
#if PG_VERSION_NUM >= 120000
	tupdesc = CreateTemplateTupleDesc(1);
#else
	tupdesc = CreateTemplateTupleDesc(1, false);
#endif
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "QUERY PLAN", result_type, -1, 0);
	return tupdesc;
}

Datum
show_plan(PG_FUNCTION_ARGS)
{
	FuncCallContext* funcctx;
	show_plan_funcctx* ctx;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext	oldcxt;
		PlannedStmt* pl_stmt = NULL;
		LOCKMODE		heap_lock = AccessShareLock;
		Relation		sr_plans_heap,
			sr_index_rel;
		Snapshot		snapshot;
		ScanKeyData		key;
		ListCell* lc;
		char* queryString;
		ExplainState* es = NewExplainState();
		uint32			index,
			query_hash = PG_GETARG_INT32(0);
		Relation* rel_array;
		int             i;

		funcctx = SRF_FIRSTCALL_INIT();

		if (!PG_ARGISNULL(1))
			index = PG_GETARG_INT32(1);	/* show by index or enabled (if 0) */
		else
			index = 0;	/* show enabled one */

		es->analyze = false;
		es->costs = false;
		es->verbose = true;
		es->buffers = false;
		es->timing = false;
		es->summary = false;
		es->format = EXPLAIN_FORMAT_TEXT;

		if (!PG_ARGISNULL(2)) {
			char* p = PG_GETARG_CSTRING(2);

			if (strcmp(p, "text") == 0)
				es->format = EXPLAIN_FORMAT_TEXT;
			else if (strcmp(p, "xml") == 0)
				es->format = EXPLAIN_FORMAT_XML;
			else if (strcmp(p, "json") == 0)
				es->format = EXPLAIN_FORMAT_JSON;
			else if (strcmp(p, "yaml") == 0)
				es->format = EXPLAIN_FORMAT_YAML;
			else
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unrecognized value for output format \"%s\"", p),
						errhint("supported formats: 'text', 'xml', 'json', 'yaml'")));
		}

		/* Try to find already planned statement */
#if PG_VERSION_NUM >= 130000
		sr_plans_heap = table_open(cachedInfo.sr_plans_oid, heap_lock);
#else
		sr_plans_heap = heap_open(cachedInfo.sr_plans_oid, heap_lock);
#endif
		sr_index_rel = index_open(cachedInfo.sr_index_oid, heap_lock);

		snapshot = RegisterSnapshot(GetLatestSnapshot());
		ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, query_hash);
		pl_stmt = lookup_plan_by_query_hash(snapshot, sr_index_rel, sr_plans_heap,
			&key, NULL, index, &queryString);
		if (pl_stmt == NULL)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("could not find saved plan")));

		rel_array = palloc(sizeof(Relation) * list_length(pl_stmt->relationOids));
		i = 0;
		foreach(lc, pl_stmt->relationOids)
#if PG_VERSION_NUM >= 130000
			rel_array[i++] = table_open(lfirst_oid(lc), heap_lock);
#else
			rel_array[i++] = heap_open(lfirst_oid(lc), heap_lock);
#endif

		ExplainBeginOutput(es);
#if PG_VERSION_NUM >= 130000
		ExplainOnePlan(pl_stmt, NULL, es, queryString, NULL, NULL, NULL, NULL);
#elif PG_VERSION_NUM >= 100000
		ExplainOnePlan(pl_stmt, NULL, es, queryString, NULL, NULL, NULL);
#else
		ExplainOnePlan(pl_stmt, NULL, es, queryString, NULL, NULL);
#endif
		ExplainEndOutput(es);
		Assert(es->indent == 0);

		UnregisterSnapshot(snapshot);
		index_close(sr_index_rel, heap_lock);
#if PG_VERSION_NUM >= 130000
		table_close(sr_plans_heap, heap_lock);
#else
		heap_close(sr_plans_heap, heap_lock);
#endif

		while (--i >= 0)
#if PG_VERSION_NUM >= 130000
			table_close(rel_array[i], heap_lock);
#else
			heap_close(rel_array[i], heap_lock);
#endif

		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		funcctx->tuple_desc = BlessTupleDesc(make_tupledesc(es));
		funcctx->user_fctx = palloc(sizeof(show_plan_funcctx));
		ctx = (show_plan_funcctx*)funcctx->user_fctx;

		ctx->format = es->format;
		ctx->output = pstrdup(es->str->data);
		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (show_plan_funcctx*)funcctx->user_fctx;

	/* if there is a string and not an end of string */
	if (ctx->output && *ctx->output) {
		HeapTuple	tuple;
		Datum		values[1];
		bool		isnull[1] = { false };

		if (ctx->format != EXPLAIN_FORMAT_TEXT) {
			values[0] = PointerGetDatum(cstring_to_text(ctx->output));
			ctx->output = NULL;
		}
		else {
			char* txt = ctx->output;
			char* eol;
			int			len;

			eol = strchr(txt, '\n');
			if (eol) {
				len = eol - txt;
				eol++;
			}
			else {
				len = strlen(txt);
				eol = txt + len;
			}

			values[0] = PointerGetDatum(cstring_to_text_with_len(txt, len));
			ctx->output = txt = eol;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, isnull);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Basic plan tree walker.
 *
 * 'visitor' is applied right before return.
 */
static void
plan_tree_visitor(Plan* plan,
	void (*visitor) (Plan* plan, void* context),
	void* context)
{
	ListCell* l;

	if (plan == NULL)
		return;

	check_stack_depth();

	/* Plan-type-specific fixes */
	switch (nodeTag(plan)) {
	case T_SubqueryScan:
		plan_tree_visitor(((SubqueryScan*)plan)->subplan, visitor, context);
		break;

	case T_CustomScan:
		foreach(l, ((CustomScan*)plan)->custom_plans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	case T_ModifyTable:
		foreach(l, ((ModifyTable*)plan)->plans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	case T_Append:
		foreach(l, ((Append*)plan)->appendplans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	case T_MergeAppend:
		foreach(l, ((MergeAppend*)plan)->mergeplans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	case T_BitmapAnd:
		foreach(l, ((BitmapAnd*)plan)->bitmapplans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	case T_BitmapOr:
		foreach(l, ((BitmapOr*)plan)->bitmapplans)
			plan_tree_visitor((Plan*)lfirst(l), visitor, context);
		break;

	default:
		break;
	}

	plan_tree_visitor(plan->lefttree, visitor, context);
	plan_tree_visitor(plan->righttree, visitor, context);

	/* Apply visitor to the current node */
	visitor(plan, context);
}

static void
execute_for_plantree(PlannedStmt* planned_stmt,
	void (*proc) (void* context, Plan* plan),
	void* context)
{
	ListCell* lc;

	proc(context, planned_stmt->planTree);

	foreach(lc, planned_stmt->subplans)
	{
		Plan* subplan = lfirst(lc);
		proc(context, subplan);
	}
}
