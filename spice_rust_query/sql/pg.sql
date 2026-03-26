CREATE TABLE IF NOT EXISTS public.city_perf (
    id BIGSERIAL PRIMARY KEY,
    city_name VARCHAR(50),
    nation VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.attacker_ip_perf (
    id BIGSERIAL PRIMARY KEY,
    attacker_ip VARCHAR(50),
    city_id BIGINT,
    city_name VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_attacker_ip_perf_attacker_ip
ON public.attacker_ip_perf (attacker_ip);

CREATE INDEX IF NOT EXISTS idx_attacker_ip_perf_city_id
ON public.attacker_ip_perf (city_id);
