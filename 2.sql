CREATE INDEX idx_resolution_exdatecur_join 
ON f_dp_resolution_exdatecur (owner, owner_type, idx);
CREATE INDEX idx_executor_idx ON executor (idx);
ANALYZE f_dp_resolution_exdatecur;
ANALYZE idx_executor_idx;
