#!/bin/bash
# 复现：跨版本 GRANT ROLE 角色短暂不可见（缓存延迟）
# 前提：Meta v1.2.879, OLD query v1.2.636 (port 3307), NEW query v1.2.887 (port 3308)
#
# 根因：OLD 的角色/权限有约 15 秒的缓存轮询机制。
#       NEW 写入的角色绑定在 meta 中是正确的，OLD 只是还没刷新缓存。
#       等待 ~16 秒后角色可见，权限恢复正常。
#
# 不需要清理 meta 数据文件，SQL 清理即可复现。

OLD=3307
NEW=3308

run_old() {
  echo "[OLD v1.2.636 root] $1"
  mysql -h127.0.0.1 -P$OLD -uroot -e "$1" 2>&1 | grep -v '\[Warning\]'
}
run_new() {
  echo "[NEW v1.2.887 root] $1"
  mysql -h127.0.0.1 -P$NEW -uroot -e "$1" 2>&1 | grep -v '\[Warning\]'
}

echo "====== 清理 ======"
run_new "DROP TABLE IF EXISTS db1.t1"
run_new "DROP USER IF EXISTS u1"
run_new "DROP ROLE IF EXISTS r1"
run_new "DROP ROLE IF EXISTS r2"
run_new "DROP DATABASE IF EXISTS db1"

echo ""
echo "====== 准备 ======"
run_new "CREATE USER u1 IDENTIFIED BY 'P@ss1'"
run_new "CREATE DATABASE db1"

echo ""
echo "====== 触发：步骤 1 — NEW 创建 r1 并授权 ======"
run_new "CREATE ROLE r1"
run_new "GRANT INSERT ON db1.* TO ROLE r1"

echo ""
echo "====== 触发：步骤 2 — OLD 将 r1 分配给 u1（OLD 写入用户角色列表）======"
run_old "GRANT ROLE r1 TO u1"
sleep 2

echo ""
echo "====== 触发：步骤 3 — NEW 创建 r2 并分配给 u1（在 OLD 写入之后）======"
run_new "CREATE ROLE r2"
run_new "GRANT CREATE ON db1.* TO ROLE r2"
run_new "GRANT ROLE r2 TO u1"
run_new "ALTER USER u1 WITH DEFAULT_ROLE = 'r2'"

echo ""
echo "====== 验证 1：立即查询（缓存未刷新，r2 不可见）======"
echo "[OLD v1.2.636 u1] SHOW ROLES — 预期: r2 不可见（缓存延迟）"
mysql -h127.0.0.1 -P$OLD -uu1 -p'P@ss1' -e "SHOW ROLES" 2>&1 | grep -v '\[Warning\]'

echo ""
echo "[OLD v1.2.636 u1] CREATE TABLE db1.t1 (x INT) — 预期: PermissionDenied"
mysql -h127.0.0.1 -P$OLD -uu1 -p'P@ss1' -e "CREATE TABLE db1.t1 (x INT)" 2>&1 | grep -v '\[Warning\]'

echo ""
echo "====== 等待 16 秒（缓存刷新周期 ~15s）======"
sleep 16

echo ""
echo "====== 验证 2：缓存刷新后（r2 可见，权限恢复）======"
echo "[OLD v1.2.636 u1] SHOW ROLES — 预期: r2 可见"
mysql -h127.0.0.1 -P$OLD -uu1 -p'P@ss1' -e "SHOW ROLES" 2>&1 | grep -v '\[Warning\]'

echo ""
echo "[OLD v1.2.636 u1] CREATE TABLE db1.t1 (x INT) — 预期: 成功"
mysql -h127.0.0.1 -P$OLD -uu1 -p'P@ss1' -e "CREATE TABLE db1.t1 (x INT)" 2>&1 | grep -v '\[Warning\]'

echo ""
echo "====== 对照组 ======"
echo "[NEW v1.2.887 u1] SHOW ROLES"
mysql -h127.0.0.1 -P$NEW -uu1 -p'P@ss1' -e "SHOW ROLES" 2>&1 | grep -v '\[Warning\]'

echo ""
echo "====== 清理 ======"
run_new "DROP TABLE IF EXISTS db1.t1"
run_new "DROP DATABASE IF EXISTS db1"
run_new "DROP USER IF EXISTS u1"
run_new "DROP ROLE IF EXISTS r1"
run_new "DROP ROLE IF EXISTS r2"
