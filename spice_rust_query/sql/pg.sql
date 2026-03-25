drop table if exists attacker_ip_perf;
CREATE TABLE if not exists attacker_ip_perf (
    id BIGSERIAL PRIMARY KEY,  -- 使用 BIGSERIAL 实现自增
    attacker_ip VARCHAR(50),
    city_id bigint,
    city_name VARCHAR(50)
);
drop table if exists city_perf;
CREATE TABLE if not exists city_perf (
    id BIGSERIAL PRIMARY KEY,  -- 使用 BIGSERIAL 实现自增
    city_name VARCHAR(50),
    nation VARCHAR(50)
);

select * from attacker_ip_perf where attacker_ip='172.152.149.97';

CREATE INDEX idx_ip
ON attacker_ip_perf (attacker_ip);

CREATE INDEX idx_city_id
ON attacker_ip_perf (city_id);