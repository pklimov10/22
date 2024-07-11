EXPLAIN ANALYZE
select * from f_dp_rkkbase rkkbase
JOIN
f_dp_rkk rkk
ON rkk.id = rkkbase.id;
