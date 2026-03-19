#ifndef ___SR_PLAN_H__
#define ___SR_PLAN_H__

#include "postgres.h"
#include "fmgr.h"
#include "string.h"
#include "optimizer/planner.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#if PG_VERSION_NUM < 120000
#include "utils/tqual.h"
#endif
#include "utils/guc.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"
#include "portability/instr_time.h"
#include "storage/lock.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "parser/analyze.h"
#include "parser/parse_func.h"
#include "tcop/utility.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "utils/syscache.h"
#include "funcapi.h"

/*
 * 知识点：数据库对象名称宏定义
 * 这些宏定义了本插件核心数据存储所依赖的系统级表和各个辅助索引的硬编码名称。
 * 将它们集中定义在头文件里，可以避免后续在各种增删改查 API 里的 "Magic String" 拼接导致写错。
 * 而且，这些宏名称与 init.sql 里的 "CREATE TABLE"、"CREATE INDEX" 名字必须是一一完全对齐的！
 */
#define SR_PLANS_TABLE_NAME	"sr_plans"
#define SR_PLANS_TABLE_QUERY_INDEX_NAME	"sr_plans_query_hash_idx"
#define SR_PLANS_RELOIDS_INDEX "sr_plans_query_oids"
#define SR_PLANS_INDEX_RELOIDS_INDEX "sr_plans_query_index_oids"

 /*
  * 知识点：函数指针 typedef 与 回调(Hook) 机制
  * deserialize_hook_type 是一个自定义回调函数的函数指针。
  * 在将 JSON(B) 文本反序列化装载还原回内部的 Node Tree（例如执行计划树）时，
  * 可能需要借助额外的环境/上下文逻辑去加工，于是提供了一个带 hook 的接口用于注入。
  */
typedef void* (*deserialize_hook_type) (void*, void*);
void* jsonb_to_node_tree(Jsonb* json, deserialize_hook_type hook_ptr, void* context);

/* 将内存结构节点树（比如执行计划）序列化打包成可在表里持久化的 JSONB 二进制结构 */
Jsonb* node_tree_to_jsonb(const void* obj, Oid fake_func, bool skip_location_from_node);

/* 一个公共遍历器，允许往各种结构体上挂载自己的回调遍历方法 */
void common_walker(const void* obj, void (*callback) (void*));

/*
 * 知识点：跨版本编译兼容 (PG_VERSION_NUM)
 * Postgres 的底层内核 API 每一代重要版本更新几乎不可避免地会有修改。
 * `PG_VERSION_NUM` 宏在预编译时由 Postgres 内核的头文件抛出。
 * 这里展示了典型的系统级扩展开发：使用条件编译（#if...#else）将差异抹平。
 *
 * MakeTupleTableSlot:
 * PG11 及以后，它变成需要明确传入 NULL 来保持之前不传参的效果。
 */
#if PG_VERSION_NUM >= 110000
#define MakeTupleTableSlotCompat() \
	MakeTupleTableSlot(NULL)
#else
#define MakeTupleTableSlotCompat() \
	MakeTupleTableSlot()
#endif

 /*
  * index_insert:
  * PG10 开始，底层直接调用 index_insert 时多强制要求了一个 IndexInfo 参数。
  * 为了老代码能平滑切过来，通过 index_insert_compat 将这一差异完全隐藏在了头文件级别。
  */
#if PG_VERSION_NUM >= 100000
#define index_insert_compat(rel,v,n,t,h,u) \
	index_insert(rel,v,n,t,h,u, BuildIndexInfo(rel))
#else
#define index_insert_compat(rel,v,n,t,h,u) index_insert(rel,v,n,t,h,u)
#endif

  /*
   * 以下是一些保证兼容旧版本或异构版本的获取和转换 JSONB 数据的参数提取宏。
   */
#ifndef PG_GETARG_JSONB_P
#define PG_GETARG_JSONB_P(x)	PG_GETARG_JSONB(x)
#endif

#ifndef PG_RETURN_JSONB_P
#define PG_RETURN_JSONB_P(x)	PG_RETURN_JSONB(x)
#endif

#ifndef DatumGetJsonbP
#define DatumGetJsonbP(d)	DatumGetJsonb(d)
#endif

   /*
	* 知识点：系统表内的列号枚举 (Attribute Numbers)
	* 在 PostgreSQL 里操作内部堆表 Tuple，找某一列并不是通过提供列的字符串名称（比如 "query_hash"），
	* 而是通过提供该列在元组定义体系中明确登记的按顺序的“列号”(Attribute Number)。
	* 这些从 1 开始计数的枚举（Anum_*）完全且必须跟 `sr_plan.sql` 里的 CREATE TABLE 结构列排布严格一对一对应。
	*
	* 注：Anum_sr_attcount 利用了 C 语言枚举递增的特性，正好作为“总列数 + 1”拿去使用和开辟内存数组。
	*/
enum
{
	Anum_sr_query_hash = 1,  /* 查询哈希 */
	Anum_sr_query_id,        /* 查询原生日志 ID */
	Anum_sr_plan_hash,       /* 执行计划哈希 */
	Anum_sr_enable,          /* 计划是否启用兜底 */
	Anum_sr_query,           /* 原始查询 SQL 文本 */
	Anum_sr_plan,            /* 计划 JSONB 文本 */
	Anum_sr_reloids,         /* 依赖表 OID 数组 */
	Anum_sr_index_reloids,   /* 依赖索引 OID 数组 */
	Anum_sr_attcount         /* 枚举终结符，主要用来代表这个表中实际存在的列的数量标记 */
} sr_plans_attributes;

#endif
