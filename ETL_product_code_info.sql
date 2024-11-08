drop table public.product_code_info ;

create table public.product_code_info(
  GOODS_ID varchar(10)
, GOODS_NM text
, MDL_CODE varchar(100)
) ;

select * from pg_tables where tablename like 'product_code_info%';

---------------------------
--Verify
---------------------------
--1) 모델명이 지수값으로 들어간 경우
select count(*) from public.product_code_info5 where mdl_code like '%E+12%';

--2) 값에 줄바꿈이 있는 경우
select * from public.product_code_info4 where GOODS_ID like '%G000293073%';

--3) PK 검증
select goods_id, count(*)
  from public.product_code_info3
group by goods_id 
having count(*) > 1 ;