-- 删除T_ZHOBTMIND表中两小时之前的数据
delete from T_ZHOBTMIND where ddatetime<sysdate-2/24;

-- 删除T_ZHOBTMIND1表中两个小时之前的数据
delete from T_ZHOBTMIND1 where ddatetime<sysdate-2/24;
commit;
exit;