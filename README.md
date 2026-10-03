# jobservice — 本地作业服务（Linux）

在 Linux 上运行**仓库内预定义的测试命令**的本地作业服务。服务不接受任意
shell 文本：调用方只能按名字引用 `jobservice/commands.py` 中注册的命令，
每个名字映射到固定的 argv 列表，以 `shell=False` 方式直接 `exec`。

## 特性

- **命令白名单**：`submit("rm -rf /")` 之类的请求直接抛 `UnknownCommand`；
  不存在 shell 解释，也就无法注入。
- **并发上限 2**：`max_concurrent` 个槽位由信号量控制，多余作业保持
  `QUEUED`，排队中即可被取消。
- **双流独立落盘**：每个作业两个读取线程分别把 stdout/stderr 流式写入
  `<data_dir>/<job_id>/{stdout,stderr}`。任一路输出持续写入都不会堵住
  另一条管道，也不会阻止取消。
- **每作业限额**：运行时间上限（`run_time_limit` 秒）与合计输出字节上限
  （stdout+stderr，`output_limit`）。
- **整组终止**：子进程以 `start_new_session=True` 启动（自成进程组组长），
  取消 / 超时 / 输出超限时 `os.killpg(SIGKILL)` 杀掉**整个进程组**（含命令
  派生的子进程），随后 `wait()` 回收并进入明确终态。
- **游标读取**：输出文件只追加，客户端按字节偏移游标读取已保存输出。
- **幂等取消**：所有状态迁移都在作业锁内完成，kill 原因 first-wins，终态
  由唯一的 worker 线程写入。重复取消返回同一个结果，绝不产生第二种结果。

## 终态机

```
QUEUED ──▶ RUNNING ──▶ SUCCEEDED              (exit code 0)
   │          ├──────▶ FAILED                 (exit code != 0)
   │          ├──────▶ CANCELED               (cancel)
   │          ├──────▶ TIMEOUT                (超过 run_time_limit)
   │          └──────▶ OUTPUT_LIMIT_EXCEEDED  (合计输出超过 output_limit)
   └────────▶ CANCELED                        (排队时被取消)
```

退出与取消竞争时，谁先拿到锁谁生效，最终恰好只有一个终态。

## API

```python
from jobservice import JobService, State

svc = JobService("/tmp/jobdata", max_concurrent=2,
                 run_time_limit=10.0, output_limit=1 << 20)

job_id = svc.submit("slow")                 # 只能提交注册的命令名
state  = svc.cancel(job_id)                 # 幂等；返回终态（或将成为的终态）
status = svc.wait(job_id, timeout=15)       # 阻塞到终态
snap   = svc.status(job_id)                 # 状态/退出码/字节数/时间戳快照

res = svc.read(job_id, "stdout", cursor=0, max_bytes=4096)
# res.data / res.next_cursor / res.eof（终态且读到末尾时为 True）

svc.shutdown()                              # 取消所有作业并回收线程
```

## 预定义命令（`commands/`）

| 名称             | 行为                                             |
|------------------|--------------------------------------------------|
| `slow`           | 每 100ms 打一行，约 60s（占槽位 / 取消 / 超时目标）|
| `spam-stderr`    | 狂刷 stderr（触发输出上限）                       |
| `spawn-children` | 派生 3 个长眠子进程并打印其 pid（验证整组终止）    |
| `exit-race`      | 打一行立即退出 0（退出与取消竞争）                |
| `mixed-output`   | 同时持续写 stdout/stderr（验证两管道互不阻塞）     |
| `fail`           | 写 stderr 后以状态码 3 退出                       |

## 运行测试

```sh
python3 -m unittest discover -s tests -v
```

覆盖：大量错误输出触发输出上限、派生子进程被整组杀掉、退出与取消竞争
只有单一结果、队列中的作业被取消且永不启动、并发上限为 2、stderr 洪水
不阻塞 stdout 与取消、游标读取重组完整输出、重复取消幂等。
