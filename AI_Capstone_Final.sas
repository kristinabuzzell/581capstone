options nosymbolgen nomlogic nomprint;

%let linkedin_csv = /home/u63898962/AI_Automation_Project/ai_ml_jobs_linkedin.csv;
%let mapping_csv  = /home/u63898962/AI_Automation_Project/linkedin_sector_mapping.csv;
%let bls_path     = /home/u63898962/AI_Automation_Project;

proc import 
    datafile="&linkedin_csv"
    out=linkedin_raw
    dbms=dlm
    replace;
  delimiter = ',';
  getnames  = yes;     
  datarow   = 2;      
  guessingrows = max;  
run;

proc contents data=linkedin_raw; run;
proc print   data=linkedin_raw(obs=5) noobs; run;

data linkedin_clean;
  set linkedin_raw(keep=sector);
  sector_u = upcase(strip(sector));
  drop sector;
run;


%macro import_and_clean(year);
  proc import
    datafile="&bls_path/natsector_M&year._dl.xlsx"
    out=raw_bls&year
    dbms=xlsx replace;
    getnames=yes;
  run;

  data bls&year;
    length NAICS $5 OCC_CODE $7 NAICS_TITLE $200;
    set raw_bls&year(rename=(TOT_EMP=TOT_EMP_char A_MEDIAN=A_MEDIAN_char));
    Year     = &year;
    TOT_EMP  = input(compress(TOT_EMP_char,,'kd'), best12.);
    A_MEDIAN = input(compress(A_MEDIAN_char,,'kd'), best12.);
    keep Year NAICS NAICS_TITLE OCC_CODE TOT_EMP A_MEDIAN;
  run;
%mend import_and_clean;

%import_and_clean(2022);
%import_and_clean(2023);
%import_and_clean(2024);

data bls_all;
  set bls2022 bls2023 bls2024;
run;

data bls_totals;
  set bls_all;
  where OCC_CODE = '00-0000';
run;

proc import
  datafile="&mapping_csv"
  out=sector_map_raw
  dbms=csv replace;
  getnames = yes;
  guessingrows = max;
run;

data sector_map;
  set sector_map_raw
      (rename=(NAICS=NAICS_num sector=sector_raw));
  sector_u = upcase(strip(sector_raw));
  NAICS = put(NAICS_num, z2.);
  keep sector_u NAICS;
run;

proc sql;
  create table ai_by_sector as
  select
    sector_u,
    count(*) as postings
  from linkedin_clean
  where sector_u ne ""
  group by sector_u
  ;
quit;

proc sql;
  create table ai_by_naics as
  select
    m.NAICS,
    sum(s.postings) as AI_Postings_By_NAICS
  from ai_by_sector as s
    inner join sector_map as m
     on index(s.sector_u, m.sector_u) > 0
  group by m.NAICS
  ;
quit;

proc sql;
  create table mapping_debug as
  select 
    s.sector_u,
    s.postings,
    m.NAICS
  from ai_by_sector as s
    left join sector_map as m
      on index(s.sector_u, m.sector_u) > 0
  ;
quit;

proc print data=mapping_debug noobs;
  title "All LinkedIn sectors → mapped NAICS (blank = unmapped)";
  var sector_u postings NAICS;
run;

proc print data=mapping_debug(where=(NAICS='')) noobs;
  title "Unmapped LinkedIn sectors (need new mapping keys)";
  var sector_u postings;
run;

proc print data=mapping_debug(where=(NAICS ne '')) noobs;
  title "Mapped LinkedIn sectors";
  var sector_u postings NAICS;
run;

proc sql;
  create table bls_naics_merged as
  select
    B.Year,
    B.NAICS,
    B.NAICS_TITLE,
    B.TOT_EMP      as Total_Employment,
    B.A_MEDIAN     as Avg_Median_Wage,
    coalesce(N.AI_Postings_By_NAICS, 0) as AI_Postings_By_Sector
  from bls_totals as B
  left join ai_by_naics as N
    on B.NAICS = N.NAICS
  order by B.Year, B.NAICS
  ;
quit;

proc sql;
  create table naics_analysis as
  select
    curr.NAICS,
    curr.Year    as Year_Curr,
    prev.Year    as Year_Prev,
    prev.Total_Employment as Emp_Prior,
    curr.Total_Employment as Emp_Curr,
    (prev.Total_Employment - curr.Total_Employment)
      / prev.Total_Employment    as Job_Loss_Pct format=percent8.2,
    prev.AI_Postings_By_Sector          as AI_Postings_Prior,
    prev.AI_Postings_By_Sector
      / prev.Total_Employment    as AI_Intensity format=8.4
  from bls_naics_merged as curr
  inner join bls_naics_merged as prev
    on curr.NAICS = prev.NAICS
   and curr.Year  = prev.Year + 1
  ;
quit;

proc sql;
  create table naics_analysis2 as
  select
    A.*,
    B.Total_Employment as Industry_Size,
    B.Avg_Median_Wage  as Avg_Wage
  from naics_analysis        as A
  left join bls_naics_merged as B
    on A.NAICS     = B.NAICS
   and A.Year_Curr = B.Year
  ;
quit;

proc means data=naics_analysis2 noprint;
  var AI_Intensity Emp_Prior Avg_Wage;
  output out=_means_ mean=mu_AI mu_Emp mu_Wage;
run;

data naics_centered;
  if _n_=1 then set _means_;
  set naics_analysis2;
  cAI   = AI_Intensity - mu_AI;
  cEmp  = Emp_Prior    - mu_Emp;
  cWage = Avg_Wage     - mu_Wage;
  cAI1000 = cAI * 1000;
  Intx_Emp   = cAI1000 * cEmp;
  Intx_Wage  = cAI1000 * cWage;
  keep NAICS Year_Curr Job_Loss_Pct
       cAI1000 cEmp cWage Intx_Emp Intx_Wage;
run;

proc reg data=naics_centered;
  model Job_Loss_Pct = cAI1000 cEmp cWage
        / vif collin;
  title "Base centered regression: Job_Loss on AI (centered) & controls";
run; quit;

proc reg data=naics_centered plots=diagnostics(unpack);
  model Job_Loss_Pct = cAI1000 cEmp cWage Intx_Emp Intx_Wage
        / vif collin;
  output out=reg_out p=Pred r=Resid;
  title "Moderation: centered AI w/ Emp & Wage interactions";
run; quit;


proc univariate data=reg_out normal;
  var Resid;
  histogram Resid / normal kernel;
  title "Residuals: normality check";
run;

options symbolgen mlogic mprint notes;

proc sgplot data=naics_analysis2;
  scatter x=AI_Intensity y=Job_Loss_Pct;
  title "Scatter Plot: AI Job Intensity vs. Job Loss Percentage";
  xaxis label="AI Job Intensity";
  yaxis label="Job Loss Percentage";
run;

proc sgplot data=reg_out;
  scatter x=Pred y=Resid;
  refline 0 / axis=y lineattrs=(pattern=shortdash);
  title "Residuals vs. Predicted Job Loss Percentage";
  xaxis label="Predicted Job Loss %";
  yaxis label="Residuals";
run;

