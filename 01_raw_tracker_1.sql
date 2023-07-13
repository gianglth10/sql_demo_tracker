with order_raw as (
    select 
    date_trunc('month',date(from_unixtime(try_cast(o.create_timestamp as bigint) - 3600))) as grass_month
    , date(from_unixtime(try_cast(o.create_timestamp as bigint) - 3600)) as grass_date
    , c.main_category
    , case 
        when o.pv_voucher_id is not null then v.voucher_type
        when o.pv_voucher_id is null then 'Unknown'
        else 'Others'
    end as voucher_type
    , coalesce(b.buyer_tier, 'Unknown') as buyer_tier
    , o.buyer_id
    , o.order_id
    , case when pv_voucher_id is not null then 1 else 0 end as is_pv_voucher
    , case when fsv_voucher_id is not null then 1 else 0 end as is_fsv_voucher

    , sum(order_fraction) gross_order_mece
    , sum(case when is_net_order = 1 then order_fraction end) net_order_mece
    , sum(gmv_usd) as gmv_usd
    , sum(case when is_net_order = 1 then gmv_usd end) nmv_usd
    , sum(prm_usd) as prm_usd
    , sum(rev_usd) as rev_usd

    from testing.order_item o
    left join testing.voucher_info v
    on o.pv_voucher_id = v.voucher_id
    left join testing.buyer_tier b
    on o.buyer_id = b.userid
    and date_trunc('month',date(from_unixtime((create_timestamp - 3600)))) = b.grass_month
    left join testing.category_mapping c
    on o.category_id = c.category_id
    group by 1,2,3,4,5,6,7,8,9
)
, order_revise as (
    select 
    grass_date
    , is_fsv_voucher
    , is_pv_voucher
    , coalesce(main_category,'Others') main_category
    , order_id
    , row_number() over (partition by grass_date, order_id, main_category order by order_id asc) as rank
    from order_raw
)
, order_check as (
    select distinct
    a.grass_date
    , a.order_id
    , a.main_category
    , 1 / try_cast(b.rank as double) as gross_order_nonmece 
    from order_revise a
    join (select main_category, order_id, max(rank) as rank from order_revise group by 1,2) b
    on a.order_id = b.order_id
    and a.main_category = b.main_category
)
, order_data as (
    select 
    a.grass_month
    , a.grass_date
    , a.main_category
    , a.voucher_type
    , a.buyer_tier
    , a.is_pv_voucher
    , a.is_fsv_voucher
    , a.buyer_id

    , count(distinct a.buyer_id) as buyer_non_mece
    , sum(gross_order_mece) as gross_order_mece
    , sum(gross_order_nonmece) gross_order_nonmece
    , sum(net_order_mece) as net_order_mece
    , sum(gmv_usd) as gmv_usd
    , sum(nmv_usd) as nmv_usd
    , sum(prm_usd) as prm_usd
    , sum(rev_usd) as rev_usd

    from order_raw a
    left join order_check b
    on a.grass_date = b.grass_date
    and a.order_id = b.order_id
    and a.main_category = b.main_category
    group by 1,2,3,4,5,6,7,8
)
select 
a.grass_month as current_month
, coalesce(a.buyer_tier, 'unknown') current_buyer_tier
, b.grass_month as next_month
, coalesce(b.buyer_tier, 'unknown') next_buyer_tier

, count(distinct a.buyer_id) buyer
, sum(gross_order_mece) as gross_order_mece
, sum(net_order_mece) as net_order_mece
, sum(gmv_usd) as gmv_usd
, sum(nmv_usd) as nmv_usd
, sum(prm_usd) as prm_usd
, sum(rev_usd) as rev_usd

from order_data a
left join (select distinct grass_month, buyer_id, buyer_tier from order_data) b
on a.buyer_id = b.buyer_id
and a.grass_month <= b.grass_month 
group by 1,2,3,4