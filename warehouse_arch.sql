create table public.t_stores (
	store_id int4 not null unique,
	store_name text not null,
	adress text not null
);

alter table public.t_store 
add primary key (sore_id);



create table public.t_categories(
cat_id int4 not null, 
cat_name text not null
);

alter table public.t_categories 
add primary key (cat_id);




create table public.t_subcategories(
subcat_id int4 not null, 
subcat_name text not null, 
bigcat_id int not null
);

alter table public.t_subcategories 
add primary key (subcat_id);

alter table public.t_subcategories 
add constraint t_categories_fk foreign key (bigcat_id)
references public.t_categories (cat_id);



create table public.t_items(
item_id	text not null,
item_name text not null,
item_cat int4 not null,
item_subcat	int4 not null
);

alter table public.t_items 
add primary key (subcat_id);

alter table public.t_items 
add constraint t_items_fk_cat foreign key (item_subcat)
references public.t_subcategories (bigcat_id);

alter table public.t_items 
add constraint t_items_fk_subcat foreign key (item_cat)
references public.t_subcategories (cat_id);



create table public.t_sales (
transaction_id text not null,
item_id text not null,
amount int4 not null,
profit numeric not null,
transaction_date timestamp not null,
store_id int4 not null,
load_date timestamp not null
);

alter table public.t_sales 
add primary key (transaction_id,item_id);

alter table public.t_sales 
add constraint t_sales_fk foreign key (item_id)
references public.t_subcategories (item_id);


create table public.tmp_sales (
	transaction_id text,
	item_id text,
	amount int4,
	profit numeric,
	transaction_date timestamp,
	store_id int4,
	load_date timestamp 
);

create table public.frod_sales (
	transaction_id text,
	item_id text,
	amount int4,
	profit numeric,
	transaction_date timestamp,
	store_id int4,
	load_date timestamp 
);
