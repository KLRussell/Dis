/* Empty Fact Table */
truncate table {DF TBL}
;

declare @EOM date = eomonth(getdate());

/* Control Center */
	/* Control flows to check for existence of all temp tables and drop those tables if exist */ 
if object_id('tempdb..#extract_tmp') is not null
	begin
    drop table #extract_tmp
end
;

if object_id('tempdb..#extract_tmp2') is not null
	begin
    drop table #extract_tmp2
end
;

if object_id('tempdb..#email_tmp') is not null
	begin
    drop table #email_tmp
end
;

if object_id('tempdb..#staging_tmp') is not null
	begin
    drop table #staging_tmp
end
;

if object_id('tempdb..#Dispute_Fact') is not null
	begin
    drop table #Dispute_Fact
end
;

if object_id('tempdb..#Map_TMP') is not null
	begin
    drop table #Map_TMP
end
;

if object_id('tempdb..#Ban_TMP') is not null
	begin
    drop table #Ban_TMP
end
;

if object_id('tempdb..#BAN_Master') is not null
	begin
    drop table #BAN_Master
end
;

if object_id('tempdb..#BAN_Master2') is not null
	begin
    drop table #BAN_Master2
end
;

if object_id('tempdb..#Dispute_Category') is not null
	begin
    drop table #Dispute_Category
end
;

if object_id('tempdb..#CPID_Tmp') is not null
begin
	drop table
		#CPID_Tmp
end
;

--------------------------------------------------------------------------------------------------------------------------------
/*Settings*/
set nocount on;
set ANSI_WARNINGS off;

--------------------------------------------------------------------------------------------------------------------------------
/* Initial Data Store - Dispute Category table */
						/* Used to Normalize Dispute Category for disputes */
create table
	#Dispute_Category
(
	Dispute_Category varchar(max),
	Norm_Dispute_Category varchar(max)
)
;

/* CTE to store distinct dispute categorys into Dispute Category Temp table */
with
	MY_TMP
As
(
	select distinct
		Dispute_Category
	from {DE TBL}
	union
	select distinct
		Dispute_Category
	from {Email TBL}
	union
	select distinct
		Dispute_Category
	from {DS TBL}
)

/* Upload distinct Dispute_Category from Extract */
insert into #Dispute_Category
(Dispute_Category)
	select distinct
		Dispute_Category
	from MY_TMP
;

/* Normalize STC Disputes */
update A
	set
		A.Norm_Dispute_Category='STC Dispute'
from #Dispute_Category A
where Dispute_Category like 'Resale Features%' or Dispute_Category like 'STC%'
;

/* Normalize GRT CNR Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT CNR'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT Unbilled%'
;

/* Normalize GRT CNR Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT CNR'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT CNR%'
;

/* Normalize GRT Price_Variance Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT Price_Variance'
from #Dispute_Category A
where replace(DISPUTE_CATEGORY,'  ',' ') like 'GRT Price%'
;

/* Normalize GRT Quantity_Variance Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT Quantity_Variance'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT Quanity%' or replace(DISPUTE_CATEGORY,'_',' ') like 'GRT Quantity%'
;

/* Normalize GRT LPC Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT LPC'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT LPC%' or DISPUTE_CATEGORY like 'GRT Paper LPC%'
;

/* Normalize GRT RAT Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT RAT Dispute'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT RAT%'
;

/* Normalize GRT LV Disputes */
update A
	set
		A.Norm_Dispute_Category='GRT LV Dispute'
from #Dispute_Category A
where DISPUTE_CATEGORY like 'GRT LV%'
;

/* Normalize Legacy Dispute Category Disputes */
update A
	set
		A.Norm_Dispute_Category='Legacy Dispute Category'
from #Dispute_Category A
where Norm_Dispute_Category is null
;

--------------------------------------------------------------------------------------------------------------------------------
/* Secondary Data Store - Extract temp table */
						/* Used to store bulk data from Dispute Extract table */
create table #extract_tmp
(
	DE_ID int,
	STC_Index int,
	Norm_Dispute_Category varchar(255),
	Claim int,
	Claim2 int,
	Open_Dispute bit,
	Claim_Type int
)
;

/* Grab data from dispute extract and create two Claim styles
						Claim styles reflect data changes with claim submission according to date logic */
with
	MY_TMP as
	(
		select
			[index],
			max(date_Updated) date_updated

		from {DE TBL}
		group by [index]
	)
,
	My_TMP2 as
	(
		select
			ID as DE_ID,
			a.[INDEX] as STC_Index,
			STC_CLAIM_NUMBER,
			BILL_DATE,
			dispute_amount,
			iif(display_status in ('Denied - Closed', 'Paid', 'Partial Paid'),0,1) Open_Dispute,
			iif(dispute_category like 'GRT%',1,0) GRT_STC,
			Dispute_Category,
			date_submitted,
			row_number()
				over(partition by a.[index], a.date_updated order by id desc) as Filter

		from {DE TBL} A
		inner join MY_TMP B
		on A.[index]=b.[index] and a.DATE_UPDATED=B.date_updated
	)

/* Insert refined data into Extract Temp table */

insert into #extract_tmp
	select
		DE_ID,
		STC_Index,
		Norm_Dispute_Category,
		dense_rank()
			over(partition by 1 order by stc_claim_number, bill_date, round(dispute_amount,2)) as Claim,
		dense_rank()
			over(partition by 1 order by stc_claim_number, bill_date) as Claim2,
		Open_Dispute,
		iif((GRT_STC=1 and date_submitted<'2/5/2016') or (GRT_STC=0 and date_submitted<'11/21/2017'),0,1) Claim_Type

	from My_TMP2 A
	inner join #Dispute_Category B
	on A.DISPUTE_CATEGORY=B.Dispute_Category

	where filter=1
;

--------------------------------------------------------------------------------------------------------------------------------
/* 3rd Data Store - Extract temp table 2 */
						/* Upload refined data from Extract Temp table */
create table #extract_tmp2
(
	Distinct_Claim int,
	DE_ID int,
	STC_Index int,
	Norm_Dispute_Category varchar(255),
	Open_Dispute bit
)
;

/* Maximize Claim_type so that comparision can be done in the proceeding insert statement */
with My_Tmp as
(
	select
		Claim,
		Claim2,
		DE_ID,
		STC_Index,
		Norm_Dispute_Category,
		Open_Dispute,
		max(Claim_Type)
				over(partition by Claim2 order by STC_Index desc) Claim_Type

	from #extract_tmp
)

/* Combine both Claim styles into one distinct claim style that reflects old data and current data */
insert into #extract_tmp2
	select
		dense_rank()
			over(partition by 1 order by iif(
				Claim!=Claim2 and Claim_Type=0,
				Claim, Claim2 * -1)) as Distinct_Claim,
		DE_ID,
		STC_Index,
		Norm_Dispute_Category,
		Open_Dispute

	from my_tmp
;

--------------------------------------------------------------------------------------------------------------------------------
/* Maximize DE_ID and create a filter to remove dupes */
with
My_Tmp as
(
	select
		Distinct_Claim,
		STC_Index,
		Norm_Dispute_Category,
		Open_Dispute,
		max(DE_ID)
			over(partition by distinct_claim order by STC_Index desc) as Dispute_ID,
		row_number()
			over(partition by distinct_claim order by STC_Index desc) as Filter,
		row_number()
			over(partition by distinct_claim order by STC_Index asc) as Index_Count

	from #extract_tmp2
),
My_Tmp2 as
(
	select
		*

	FROM My_Tmp

	where filter=1
),
My_Tmp3 as
(
	select
		Distinct_Claim,
		STC_Index Prev_STC_Index

	FROM My_Tmp

	where filter=2
)

/* filter dupes and insert into Dispute Fact table */
insert into {DF TBL}
(Dispute_Tbl, Dispute_ID, #_of_Escalations, Prev_STC_Index, STC_Index, Norm_Dispute_Category, Open_Dispute)

	select
		'Dispute_Extract' Dispute_Tbl,
		Dispute_ID,
		Index_Count-1,
		Prev_STC_Index,
		STC_Index,
		Norm_Dispute_Category,
		Open_Dispute

	FROM My_Tmp2 A
	left join My_Tmp3 B
	on A.Distinct_Claim=B.Distinct_Claim
;

--------------------------------------------------------------------------------------------------------------------------------
/* 4th Data Store - email temp table */
						/* Store bulk data from email extract table */
create table #email_tmp
(
	Table_ID int,
	Date_Updated datetime,
	Open_Dispute bit,
	distinct_claim int,
	GRT_Index int,
	Dispute_Category varchar(255)
)
;

/* Creates a GRT Index according to a pattern and tokenize display_status */
insert into #email_tmp
select
	ID Table_ID,
	Date_Updated,
	iif(display_status in ('Denied - Closed', 'Paid', 'Partial Paid', 'Rejected'),0,1) Open_Dispute,
	dense_rank()
		OVER(PARTITION BY 1 order by stc_claim_number, bill_date, Dispute_Amount) distinct_claim,
	dense_rank()
		OVER(PARTITION BY 1 order by stc_claim_number, bill_date, Dispute_Amount, Confidence_Level, Source, LEFT(Dispute_Category,3), ISNULL(Dispute_Reason,'')) GRT_INDEX,
	Dispute_Category

FROM {Email TBL}
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to temporary store max Table ID and create filter for data in email temp table */
with
My_TMP AS
(
	select
		Table_ID,
		Open_Dispute,
		row_number()
				over(partition by GRT_INDEX order by Date_Updated desc) as Filter,
		dense_rank()
				over(partition by distinct_claim order by GRT_INDEX asc) as Index_Num,
		Dispute_Category
	from #email_tmp
)

/* insert records from CTE to Dispute Fact table */
insert into {DF TBL}
(Dispute_Tbl, Dispute_ID, #_of_Escalations, Open_Dispute, Norm_Dispute_Category)

	select
		'Email_Extract' Dispute_Tbl,
		Table_ID,
		Index_Num-1,
		Open_Dispute,
		Norm_Dispute_Category

	from My_TMP A
	inner join #Dispute_Category B
	on A.Dispute_Category=B.Dispute_Category

	where filter=1
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to grab DF_ID and STC_Index by Dispute table of Dispute Extract */
	/* This section is meant to MAP Dispute Extract disputes to BMI ID and DS_ID */
with
My_TMP as
(
	select
		DF_ID,
		STC_Index

	from {DF TBL}

	where Dispute_Tbl='Dispute_Extract'
)

/* Grabs DS_ID and BMI_ID from Dispute Extract Bridge table and appends information to Dispute Fact table according to DF_ID */
update C
	set
		C.DS_ID=B.DS_ID,
		C.Source_TBL=iif(B.BMI_ID is null,NULL,'BMI'),
		C.Source_ID=B.BMI_ID

	from MY_TMP A
	inner join {DEB TBL} B
	on A.stc_index=b.[index]
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID
;

--------------------------------------------------------------------------------------------------------------------------------
/* 5th Data Store - staging temp table */
						/* Store bulk data from dispute staging table */
create table #staging_tmp
(
	DS_ID int,
	Filter int
)
;

/* Grab all DS_ID from Dispute Fact table */
insert into #staging_tmp
	select
		DS_ID,
		2 as Filter

	from {DF TBL}

	where Dispute_Tbl='Dispute_Extract'
;

/* Grab all DS_ID from Dispute staging table that is batched 1/1/2016 and beyond */
insert into #staging_tmp
	select
		ID DS_ID,
		1 as Filter

	from {DS TBL}

	where batch>20160000
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to distinguish whether ID in dispute staging has records in Dispute Extract table */
	/* 2nd CTE filters out dispute staging ids that has records in Dispute Extract table */
with
MY_TMP as
(
	select
		DS_ID,
		max(Filter) Filter

	from #staging_tmp

	group by DS_ID
)
,
MY_TMP2 as
(
	select
		DS_ID

	from MY_TMP

	where Filter=1
)

/* Adds records to Dispute Fact table where items that was sent to stc is not before 1/1/2018 */
insert into {DF TBL}
(Dispute_Tbl, Dispute_ID, #_of_Escalations, Open_Dispute, Norm_Dispute_Category)
	select
		'Dispute_Staging' Dispute_Tbl,
		ID Dispute_ID,
		0,
		iif(isnull(status,'')='Rejected',0,1) Open_Dispute,
		Norm_Dispute_Category

	from MY_TMP2 A
	inner join {DS TBL} B
	on A.DS_ID=B.ID
	inner join #Dispute_Category C
	on B.Dispute_Category=C.Dispute_Category

	where not (isnull(status,'')='Delivered to STC' and batch<20180000)
;

--------------------------------------------------------------------------------------------------------------------------------
/* 6th Data Store - Ban temp table */
						/*  */
create table #Ban_TMP
(
	BanMaster_ID int,
	BAN varchar(100),
	Bill_Date date,
	Invoice_Date date
)
;

/* 7th Data Store - Ban Master table */
						/* Store data from BAN Master table to reference whether dispute BAN is invalid */
create table #BAN_Master
(
	BanMaster_ID int,
	BAN varchar(100),
	Cust_Code varchar(5),
	START_DATE date,
	END_DATE date
)
;

--------------------------------------------------------------------------------------------------------------------------------
/* Grab BAN & Bill_Date from all 3 dispute tables so that BANMaster_ID can be found */
with
	MY_TMP
As
(
	select distinct ACCOUNT_NUMBER, bill_date, eomonth(Bill_Date) invoice_date
	from {DE TBL}
	union
	select distinct BAN, bill_date, eomonth(Bill_Date) invoice_date
	from {DS TBL}
	union
	select distinct BAN, bill_date, eomonth(Bill_Date) invoice_date
	from {Email TBL}
)

insert into #Ban_TMP
(BAN, Bill_Date, Invoice_Date)
	select distinct
		*

	from MY_TMP;

--------------------------------------------------------------------------------------------------------------------------------
/* Grabs data from BanMaster table and grabs essential elements from the table */
insert into #BAN_Master
	select
		ID BanMaster_ID,
		BAN,
		Cust_Code,
		START_DATE,
		END_DATE

	FROM {BM TBL}
;

--------------------------------------------------------------------------------------------------------------------------------
/* Checks to see if BAN in Map_Tmp is found in the BanMaster table by comparing ban and bill_date between the start and end dates */
	update A
		set
			A.BanMaster_ID=B.BanMaster_ID

	from #Ban_TMP A
	inner join #BAN_Master B
	on A.BAN=B.BAN and A.Invoice_Date BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
;

/* Checks to see if BAN in Map_Tmp is found in the BanMaster table by comparing ban+cust_code and bill_date between the start and end dates */
	/* table will correct the ban in the Map_TMP by removing the cust_code */
update A
	set
		A.BanMaster_ID=B.BanMaster_ID

	from #Ban_TMP A
	inner join #BAN_Master B
	on A.BAN=B.BAN+B.Cust_Code and A.Invoice_Date BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)

	where A.BanMaster_ID is null
;

/* Checks to see if BAN in Map_Tmp is found in the BanMaster table by dropping leading zeros from BanMaster table and comparing bill_date between the start and end dates */
	/* table will correct the ban in the Map_TMP by using the BAN with the leading zeros in the BanMaster table */
update A
	set
		A.BanMaster_ID=B.BanMaster_ID

	from #Ban_TMP A
	inner join #BAN_Master B
	on SUBSTRING(B.BAN, PATINDEX('%[^0]%', B.BAN+'.'), LEN(B.BAN))=A.BAN and A.Invoice_Date BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)

	where A.BanMaster_ID is null
;

--------------------------------------------------------------------------------------------------------------------------------
/* Grabs distinct BAN, Bill_Date, and Customer_Code from Invoice Summary table */
with
MY_TMP as
(
	select distinct
		BAN,
		eomonth(bill_date) Invoice_Date,
		Customer_Code

	from {IS TBL}
)

/* Compares BAN from Map_TMP to the BAN+Cust_Code from Invoice Summary table. Thereafter, Invoice Summary table compares the ban and bill date in BanMaster  */
	/* Thereafter, the BAN will be corrected in the My_Tmp table */
update A
	set
		A.BanMaster_ID=C.BanMaster_ID

	from #Ban_TMP A
	inner join MY_TMP B
	on A.BAN=B.BAN+B.Customer_Code and A.Invoice_Date=B.Invoice_Date
	inner join #BAN_Master C
	on B.BAN=C.BAN and B.Invoice_Date BETWEEN C.Start_Date AND ISNULL(C.End_Date, @EOM)

	where A.BanMaster_ID is null
;

/* Remove NULL entries. This table will be used to map BANMaster_ID to DF_ID in Dispute Fact table later on */
delete
from #Ban_TMP
where BanMaster_ID is null

--------------------------------------------------------------------------------------------------------------------------------
/* Update Dispute Fact table to map DF_ID to BanMaster ID according to Dispute Extract */
update A
	set A.BanMaster_ID=C.BanMaster_ID
from {DF TBL} A
inner join {DE TBL} B
on A.Dispute_ID=B.ID
inner join #Ban_TMP C
on B.ACCOUNT_NUMBER=C.BAN and B.BILL_DATE=C.BILL_DATE
where Dispute_Tbl='Dispute_Extract' and A.BanMaster_ID is null

/* Update Dispute Fact table to map DF_ID to BanMaster ID according to Email Extract */
update A
	set A.BanMaster_ID=C.BanMaster_ID
from {DF TBL} A
inner join {Email TBL} B
on A.Dispute_ID=B.ID
inner join #Ban_TMP C
on B.BAN=C.BAN and B.BILL_DATE=C.BILL_DATE
where Dispute_Tbl='Email_Extract' and A.BanMaster_ID is null

/* Update Dispute Fact table to map DF_ID to BanMaster ID according to Dispute Staging */
update A
	set A.BanMaster_ID=C.BanMaster_ID
from {DF TBL} A
inner join {DS TBL} B
on A.Dispute_ID=B.ID
inner join #Ban_TMP C
on B.BAN=C.BAN and B.BILL_DATE=C.BILL_DATE
where Dispute_Tbl='Dispute_Staging' and A.BanMaster_ID is null

--------------------------------------------------------------------------------------------------------------------------------
/* 8th Data Store - Dispute Fact table */
						/* Store bulk data from dispute table table */
create table #Dispute_Fact
(
	DF_ID int,
	Dispute_Tbl varchar(50),
	Dispute_ID int,
	BANMaster_ID int,
	DS_ID int,
	Norm_Dispute_Category varchar(255)
)
;

/* 9th Data Store - Map Temp table */
						/* Store data from Disputes in Email, Staging, Extract to get assential information to map to BMI/PCI */
create table #Map_TMP
(
	Dispute_Tbl varchar(50),
	Dispute_ID int,
	DF_ID int,
	BAN varchar(100),
	Bill_Date date,
	WTN varchar(255)
)
;

--------------------------------------------------------------------------------------------------------------------------------
/* Grab data from Dispute Fact table where BMI/PCI isn't populated */

insert into #Dispute_Fact
	select
		DF_ID,
		Dispute_Tbl,
		Dispute_ID,
		BanMaster_ID,
		DS_ID,
		Norm_Dispute_Category

	from {DF TBL}

	where source_id is null and BanMaster_ID is not null
;

--------------------------------------------------------------------------------------------------------------------------------
/* Join together Dispute Fact table and Dispute Extract table to grab BAN, Bill_Date, WTN, and to find the 9th and 10th digit of the stc_claim_number and store data in CTE */
with
MY_TMP as
(
	select
		B.ID Dispute_ID,
		DF_ID,
		C.BAN,
		eomonth(B.BILL_DATE) BILL_DATE,
		iif(WTN='',NULL,WTN) WTN

	from #Dispute_Fact A
	inner join {DE TBL} B
	on A.Dispute_Tbl='Dispute_Extract' and A.Dispute_ID=B.ID
	inner join {BM TBL} C
	on A.BANMaster_ID=C.ID
	left join {DS TBL} D
	on A.DS_ID=D.ID

	where Norm_Dispute_Category='GRT CNR' or Norm_Dispute_Category='GRT Price_Variance' or Norm_Dispute_Category='GRT Quantity_Variance' or D.Audit_Type='CNR Audit'
)

/* Grab data from CTE where Dispute_Category is 'GRT CNR' or Audit_Type is 'CNR Audit' */
insert into #Map_TMP
(Dispute_Tbl, Dispute_ID, DF_ID, BAN, Bill_Date, WTN)

	select
		'Dispute_Extract' Dispute_Tbl,
		Dispute_ID,
		DF_ID,
		BAN,
		BILL_DATE,
		WTN

	from MY_TMP
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to join Dispute Staging and Dispute Fact tables. This grabs necessary information to map to BMI/PCI and finds the 9th and 10th digits of stc_claim_number */
with
MY_TMP as
(
	select
		B.ID Dispute_ID,
		DF_ID,
		C.BAN,
		eomonth(BILL_DATE) BILL_DATE,
		iif(USI='',NULL,USI) WTN

	from #Dispute_Fact A
	inner join {DS TBL} B
	on A.Dispute_Tbl='Dispute_Staging' and A.Dispute_ID=B.ID
	inner join {BM TBL} C
	on A.BANMaster_ID=C.ID

	where Norm_Dispute_Category='GRT CNR' or Norm_Dispute_Category='GRT Price_Variance' or Norm_Dispute_Category='GRT Quantity_Variance' or Audit_Type='CNR Audit'
)

/* Inserts into #Map_Tmp table */
	/* Grabs data from MY_TMP CTE where digit one has "_" and digit two is numeric or is "X" or is "M". This represents MRC or PaperCost dispute */
insert into #Map_TMP
(Dispute_Tbl, Dispute_ID, DF_ID, BAN, Bill_Date, WTN)

	select
		'Dispute_Staging' Dispute_Tbl,
		Dispute_ID,
		DF_ID,
		BAN,
		BILL_DATE,
		WTN

	from MY_TMP
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to join Email Extract and Dispute Fact tables. This grabs necessary information to map to BMI/PCI and finds the 9th and 10th digits of stc_claim_number */
with MY_TMP as
(
	select
		B.ID Dispute_ID,
		DF_ID,
		C.BAN,
		eomonth(BILL_DATE) BILL_DATE,
		iif(USI='',NULL,USI) WTN

	from #Dispute_Fact A
	inner join {Email TBL} B
	on A.Dispute_Tbl='Email_Extract' and A.Dispute_ID=B.ID
	inner join {BM TBL} C
	on A.BANMaster_ID=C.ID

	where Norm_Dispute_Category='GRT CNR' or Norm_Dispute_Category='GRT Price_Variance' or Norm_Dispute_Category='GRT Quantity_Variance' or Audit_Type='CNR Audit'
)

/* Inserts into #Map_Tmp table */
	/* Grabs data from MY_TMP CTE where digit one has "_" and digit two is numeric or is "X" or is "M". This represents MRC or PaperCost dispute */
insert into #Map_TMP
(Dispute_Tbl, Dispute_ID, DF_ID, BAN, Bill_Date, WTN)

	select
		'Email_Extract' Dispute_Tbl,
		Dispute_ID,
		DF_ID,
		BAN,
		BILL_DATE,
		WTN

	from MY_TMP
;

--------------------------------------------------------------------------------------------------------------------------------
/* Update Dispute Fact table where Map_Tmp elements match BMI Table by WTN */
	/* BMI ID will be updated and source table will be BMI */
update C
	set
		C.Source_TBL='BMI',
		C.Source_ID=B.BMI_ID

	from #Map_TMP A
	inner join sbfincost.BDT_MRC_Inventory_BMI B
	on A.WTN=B.WTN and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;

/* Update Dispute Fact table where Map_Tmp elements match BMI Table by Circuit_ID */
	/* BMI ID will be updated and source table will be BMI */
update C
	set
		C.Source_TBL='BMI',
		C.Source_ID=B.BMI_ID

	from #Map_TMP A
	inner join sbfincost.BDT_MRC_Inventory_BMI B
	on A.WTN=B.Circuit_ID and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;

/* Update Dispute Fact table where Map_Tmp elements match BMI Table by BTN */
	/* BMI ID will be updated and source table will be BMI */
update C
	set
		C.Source_TBL='BMI',
		C.Source_ID=B.BMI_ID

	from #Map_TMP A
	inner join sbfincost.BDT_MRC_Inventory_BMI B
	on A.WTN=B.BTN and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;

--------------------------------------------------------------------------------------------------------------------------------
/* Update Dispute Fact table where Map_Tmp elements match BMI Table by WTN */
	/* PCI ID will be updated and source table will be PCI */
update C
	set
		C.Source_TBL='PCI',
		C.Source_ID=B.ID

	from #Map_TMP A
	inner join {PC TBL} B
	on A.WTN=B.WTN and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;

/* Update Dispute Fact table where Map_Tmp elements match BMI Table by Circuit_ID */
	/* PCI ID will be updated and source table will be PCI */
update C
	set
		C.Source_TBL='PCI',
		C.Source_ID=B.ID

	from #Map_TMP A
	inner join {PC TBL} B
	on A.WTN=B.Circuit_ID and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;

/* Update Dispute Fact table where Map_Tmp elements match BMI Table by BTN */
	/* PCI ID will be updated and source table will be PCI */
update C
	set
		C.Source_TBL='PCI',
		C.Source_ID=B.ID

	from #Map_TMP A
	inner join {PC TBL} B
	on A.WTN=B.BTN and A.BAN=B.BAN and A.BILL_DATE BETWEEN B.Start_Date AND ISNULL(B.End_Date, @EOM)
	inner join {DF TBL} C
	on A.DF_ID=C.DF_ID

	where C.Source_ID is null
;
--------------------------------------------------------------------------------------------------------------------------------
/* CTE Maps INDEX to the last GRT action ID that was pushed by GRT */
with
My_Tmp as
(
	select [INDEX] STC_Index, ID Last_GRT_Action_ID
	from
	(
		select
			[index],
			max(date_updated) date_updated,
			max(ID) ID

		from {DE TBL}

		where SOURCE_FILE like 'GRT%'

		group by [index]
	) A
)

/* Update Dispute Fact table with the last GRT Action ID */
Update A
set
	A.Last_GRT_Action_ID=B.Last_GRT_Action_ID
from {DF TBL} A
inner join My_Tmp B
on A.STC_Index=B.STC_Index
;

--------------------------------------------------------------------------------------------------------------------------------
/* CTE to MAP INDEX to the last escalation ID that was pushed by GRT */
with
My_Tmp as
(
	select [INDEX] STC_Index, ID Last_Escalation_ID
	from
	(
		select
			[index],
			max(date_updated) date_updated,
			max(ID) ID

		from {DE TBL}

		where SOURCE_FILE like 'GRT%' and Display_Status='Filed'

		group by [index]
	) A
)

/* Update Dispute Fact table with the Last Escalation ID */
Update A
set
	A.Last_Escalate_ID=B.Last_Escalation_ID,
	A.Last_GRT_Action_ID=iif(A.Last_GRT_Action_ID is null,B.Last_Escalation_ID,Last_GRT_Action_ID)
from {DF TBL} A
inner join My_Tmp B
on A.Prev_STC_Index=B.STC_Index
;

--------------------------------------------------------------------------------------------------------------------------------
/* Product Type & CPID Mapping Algorithm */

	/* CPID Temp table creation */
create table
	#CPID_Tmp
(
	DF_ID int,
	Record_Type varchar(20),
	Seed_Type char(1),
	Seed int,
	BAN varchar(100),
	Norm_Vendor varchar(100),
	State varchar(100),
	USI varchar(255),
	Invoice_Date date,
	USOC varchar(100),
	Billed_Amt money,
	Dispute_Amount money
);

	/* BanMaster 2 Temp table creation */
create table
	#Ban_Master2
(
	BanMaster_ID int,
	BAN varchar(100),
	Start_Date date,
	End_Date date,
	VendorMasterID int,
	Vendor varchar(100),
	Platform varchar(100),
	State varchar(30)
);

	/* CTE to grab necessary dispute information for product type mapping */
with
	MY_TMP
As
(
	select
		DF_ID,
		F.BAN,
		G.Vendor Norm_Vendor,
		F.State,
		isnull(B.USI,isnull(C.WTN,isnull(D.USI,E.USI))) USI,
		isnull(B.Record_Type,E.Record_Type) Record_Type,
		isnull(B.STC_CLAIM_NUMBER,isnull(D.STC_CLAIM_NUMBER,isnull(E.STC_CLAIM_NUMBER,C.STC_CLAIM_NUMBER))) Stc_Claim_Number,
		eomonth(isnull(B.Bill_Date,isnull(C.Bill_Date,D.Bill_Date))) Invoice_Date,
		isnull(B.USOC,isnull(D.USOC,E.USOC)) USOC,
		isnull(B.Billed_Amt,E.Billed_Amt) Billed_Amt,
		isnull(B.Claimed_Amt,isnull(C.Dispute_Amount,D.Dispute_Amount)) Dispute_Amount

	from {DF TBL} A
	left join {DS TBL} B
	on A.Dispute_Tbl='Dispute_Staging' and A.Dispute_ID=B.ID
	left join {DE TBL} C
	on A.Dispute_Tbl='Dispute_Extract' and A.Dispute_ID=C.ID
	left join {Email TBL} D
	on A.Dispute_Tbl='Email_Extract' and A.Dispute_ID=D.ID
	left join {DS TBL} E
	on A.DS_ID is not null and A.DS_ID=E.ID
	left join {BM TBL} F
	on A.BanMaster_ID=F.ID
	left join sbfincost.TAT_Lkup_VendorMaster G
	on F.VendorMasterID=G.ID
)

	/* This strips stc_claim_number into seed record and seed plus take information from CTE and inserts into temp table */
insert into #CPID_Tmp
(DF_ID,Record_Type,Seed_Type,Seed,BAN,Norm_Vendor,State,USI,Invoice_Date,USOC,Billed_Amt,Dispute_Amount)
	select
		DF_ID,
		Record_Type,
		iif(patindex('%[A-Z]|%',reverse(replace(Stc_Claim_Number,'_','|'))) - 1 > 0 and patindex('%[A-Z]|%',reverse(replace(Stc_Claim_Number,'_','|'))) = patindex('%[^0-9]%',reverse(stc_claim_number)),substring(reverse(stc_claim_number),patindex('%[^0-9]%',reverse(stc_claim_number)),1),NULL) Seed_Type,
		iif(patindex('%[A-Z]|%',reverse(replace(Stc_Claim_Number,'_','|'))) - 1 > 0 and patindex('%[A-Z]|%',reverse(replace(Stc_Claim_Number,'_','|'))) = patindex('%[^0-9]%',reverse(stc_claim_number)),replace(reverse(left(reverse(replace(Stc_Claim_Number,'_','|')), patindex('%[A-Z]|%',reverse(replace(Stc_Claim_Number,'_','|'))) - 1)),'|','_'),NULL) Seed,
		BAN,
		Norm_Vendor,
		State,
		USI,
		Invoice_Date,
		USOC,
		Billed_Amt,
		Dispute_Amount

	from MY_TMP;

update B
	set
		B.Seed_Type = A.Seed_Type,
		B.Seed = A.Seed

from #CPID_Tmp A
inner join {DF TBL} B
on
	A.DF_ID = B.DF_ID

where
	A.Seed_Type is not null
		and
	A.Seed is not null;

	/* Insert Data from Banmaster and store into #BanMaster */
insert into #Ban_Master2
	select
		A.ID BanMaster_ID,
		BAN,
		Start_Date,
		isnull(End_Date, @EOM) End_Date,
		VendorMasterID,
		Vendor,
		Platform,
		State

	from {BM TBL} A
	inner join SbFinCost.TAT_Lkup_PlatformMaster B
	on
		A.PlatformMasterID = B.ID
	inner join sbfincost.TAT_Lkup_VendorMaster C
	on
		A.VendorMasterID = C.ID;

	/* Maps CPID to MRC Seed via MRC CMP table */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is null
) > 0
begin
	update B
	set
		B.CPID = C.CPID

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join {MC TBL} C
	on
		A.Seed = C.BDT_MRC_ID
			and
		A.Invoice_Date = C.Invoice_Date
	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is null;
end;

	/* Maps CPID by VendorMasterID, State, USOC, and USOC_Desc via CPID table */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is null
			and
		A.USOC is not null
) > 0
begin
	update B
		set
			B.CPID = D.ID

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join #Ban_Master2 C
	on
		B.BanMaster_ID = C.BanMaster_ID
	inner join {CPID TBL} D
	on
		D.Source_Table = 'MRC'
			and
		D.VendorMasterID = C.VendorMasterID
			and
		D.State = A.State
			and
		D.USOC = A.USOC

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is null
			and
		A.USOC is not null;
end;

	/* Maps Product Type by CPID using the CPID Categorization table */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is not null
			and
		B.Product_Type is null
) > 0
begin
	update B
		set
			B.Product_Type = D.Norm_Product_Type

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join {CPIDCAT TBL} C
	on
		B.CPID = C.CPID
	inner join {CPN TBL} D
	on
		C.Fin_Prod_Code = D.Product_Type

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.CPID is not null
			and
		B.Product_Type is null
			and
		C.Fin_Prod_Code is not null;
end;

	/* Check BMM table to see if Product Type exists on a BMI ID level and append Product Type */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		B.Product_Type is null
			and
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
) > 0
begin
	with
		BMM
	As
	(
		select
			BMI_ID,
			Norm_Product_Type

		from {BMM TBL} A
		inner join {CPN TBL} B
		on
			A.gs_srvType = B.Product_Type

		where
			gs_srvType is not null
				and
			gs_srvType != 'BMB'
	)

	update B
		set
			B.Product_Type = C.Norm_Product_Type

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join BMM C
	on
		B.Source_ID = C.BMI_ID

	where
		B.Product_Type is null
			and
		B.Source_ID is not null
			and
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		);
end;

	/* Check BMB table to see if Product Type exists on BMI ID level and append Product Type */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		B.Product_Type is null
			and
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
) > 0
begin
	with
		BMB
	As
	(
		select
			BMI_ID,
			Norm_Product_Type

		from {BMB TBL} A
		inner join {CPN TBL} B
		on
			A.gs_srvType = B.Product_Type

		where
			gs_srvType is not null
	)

	update B
		set
			B.Product_Type = C.Norm_Product_Type

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join BMB C
	on
		B.Source_ID = C.BMI_ID

	where
		B.Product_Type is null
			and
		B.Source_ID is not null
			and
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		);
end;

	/* Map Product Type by utilizing the MIL table on a BAN level */
if
(
	select
		count(*) #

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.Product_Type is null
) > 0
begin
	WITH
		MY_TMP
	As
	(
		SELECT DISTINCT
		BAN,
		isnull(B.Norm_Product_Type,'Other') Norm_Product_Type

		FROM {MIL TBL} A
		left join {CPN TBL} B
		on
			A.Product = B.Product_Type

		WHERE
			Product <> 'Invoice Charge'
	),
		MY_TMP2
	As
	(
		select distinct
			BAN,
			iif
			(
				count(Norm_Product_Type)
					over
					(
						Partition by BAN
					) > 1,
					'Multi',
					Norm_Product_Type
			) Norm_Product_Type

		from MY_TMP
	)

	update B
		set
			B.Product_Type = C.Norm_Product_Type

	from #CPID_Tmp A
	inner join {DF TBL} B
	on
		A.DF_ID = B.DF_ID
	inner join MY_TMP2 C
	on
		A.BAN = C.BAN

	where
		(
			A.Record_Type = 'MRC'
				or
			A.Seed_Type = 'M'
		)
			and
		B.Product_Type is null
			and
		C.Norm_Product_Type != 'Multi';
end;

update A
	set
		A.Product_Type = iif(B.Record_Type = 'MRC' or B.Seed_Type = 'M', 'Other', 'Non-MRC Other')

from {DF TBL} A
inner join #CPID_Tmp B
on
	A.DF_ID = B.DF_ID

where
	Product_Type is null;

--------------------------------------------------------------------------------------------------------------------------------
/* Temp table cleanup */
drop table
	#extract_tmp,
	#extract_tmp2,
	#email_tmp,
	#staging_tmp,
	#Dispute_Fact,
	#Map_TMP,
	#BAN_Master,
	#Ban_Master2,
	#CPID_Tmp,
	#Ban_TMP,
	#Dispute_Category
;
