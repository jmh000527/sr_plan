# sr_plan (PostgreSQL 执行计划锁定插件) 使用指南

`sr_plan` 是一款类似 Oracle Outline 系统的 PostgreSQL 数据库插件。它可以拦截并保存指定 SQL 的执行计划，使得数据库在未来遇到相同的 SQL 时，强制复用被保存的计划，而不去依赖优化器当下的自动（有时甚至是不准确）估算，从而实现真正的**计划锁定 (Query Plan Locking)**。

## 一、安装与初始化

1.  **编译与安装扩展**
    在系统终端的插件源码目录下执行：
    ```bash
    make USE_PGXS=1
    make USE_PGXS=1 install
    ```

2.  **配置生效**
    修改 PostgreSQL 的配置文件 `postgresql.conf`，将本插件加入到预加载库中去。如果还有其他涉及底层干预的插件 (例如 `pg_stat_statements`)，建议将 `sr_plan` 放在它们前面。
    ```ini
    shared_preload_libraries = 'sr_plan, pg_stat_statements'
    ```
    *重启 PostgreSQL 数据库使配置生效。*

3.  **在目标数据库中挂载并启用**
    连接进入你想使用的具体数据库：
    ```sql
    CREATE EXTENSION sr_plan;
    ```
    安装成功后，系统内会自动生成一张 `sr_plans` 的控制表及相应的辅助函数。

---

## 二、基础用法：捕获与锁定固定 SQL

如果您想锁定某条确切语句的计划：

**第一步：开启“计划写入模式”**
在当前的 Session 会话连结里设置变量开启，这意味着接下来你在本机敲的所有执行查循，其产生的底层计划树都会被它默默录入到 `sr_plans` 的系统表中。
```sql
SET sr_plan.write_mode = true;
```

**第二步：执行你想锁定的那条 SQL**
此时需要构造条件，让优化器给出最佳计划并执行。
```sql
SELECT count(1) FROM users WHERE age = 10;
```

**第三步：关闭写入模式，以防污染**
一旦所需的卓越的执行计划被写入系统表后，必须立即关掉此模式。
```sql
SET sr_plan.write_mode = false;
```

**第四步：启用该计划的强绑定**
默认情况下保存下来的计划是“关闭(false)”状态的，需要您去 `sr_plans` 表中人为确认其开启。
```sql
-- 可先查看刚才保存的都有哪些计划：
SELECT query_hash, query, enable FROM sr_plans;

-- 确定了刚才想锁定的那条，更新它为强制启用状态 (假定只有一条)
UPDATE sr_plans SET enable = true WHERE query LIKE '%users WHERE age = 10%';
```
自此以后，只要系统再出现 `SELECT count(1) FROM users WHERE age = 10;`，都不用再走一遍计划评估，直接调用绑定的极速物理计划操作。

---

## 三、高级用法：参数化通用查询捕获 (伪函数 `_p` 的运用)

现实业务中往往查的并不总等于 ”10“，而是无数个未知变量。
为了让同样的结构（仅仅是数字改变）能共享同一份计划，插件提供了 `_p(anyelement)` 占位符。

**1. 开始录制通用计划：**
```sql
SET sr_plan.write_mode = true;

-- 将可能变动的参数值用 _p() 假函数包裹起来
SELECT count(1) FROM users WHERE age = _p(10) AND status = _p(1);

SET sr_plan.write_mode = false;
```

**2. 在表中激活这套通用计划：**
```sql
UPDATE sr_plans SET enable = true WHERE query LIKE '%_p(10)%';
```

**3. 复用成果：**
以后所有同样包裹在 _p 中的同构查询，不管内联传了何值，引擎都能完美调用。
```sql
-- 这些查询均会直接继承之前为那条 _p(10) 准备的执行计划
SELECT count(1) FROM users WHERE age = _p(20) AND status = _p(3);
SELECT count(1) FROM users WHERE age = _p(99) AND status = _p(0);
```

---

## 四、查看被保存的底层执行计划

如果您想看看究竟是怎样的“优秀神仙计划”被存进了 `sr_plans` 这张表中，您可以利用提供的专属展示工具而不是执行 `EXPLAIN`。

首先通过 `SELECT query_hash FROM sr_plans;` 获取你保存语句的 哈希ID（例如是 10045）。

**只展示被 Enable (激活) 的计划文本：**
```sql
SELECT show_plan(10045);
```

**展示对应此 SQL hash 收录的按顺序排列的第二条历史计划选项：**
```sql
SELECT show_plan(10045, index := 2);
```

**用特定的格式导出用于分析（支持 `json`, `text`, `xml`, `yaml`）：**
```sql
SELECT show_plan(10045, format := 'json');
```

---

## 五、运行时监控与日志调试

如果你想知道在系统的海量并发中，当前正在跑的 SQL 到底有没有成功命中了 `sr_plan` 的复用机制，可以使用控制台或全局日志开关进行验证：

```sql
-- 将命中和收集参数的状态输出到日志控制台/或返回客户端 NOTICE
SET sr_plan.log_usage = NOTICE;
```
*(选项包含：none, debug, log, info, notice, warning 等常见模式, notice 等。设为 none 取消监控)。*