options nosymbolgen nomlogic nomprint;

* Set filepaths;
%let linkedin_csv = /home/u63898962/AI_Automation_Project/ai_ml_jobs_linkedin.csv;
%let mapping_csv  = /home/u63898962/AI_Automation_Project/linkedin_sector_mapping.csv;
%let bls_path     = /home/u63898962/AI_Automation_Project;

* Import LinkedIn data;
proc import datafile="&linkedin_csv" out=linkedin_raw dbms=dlm replace;
  delimiter = ',';
  getnames  = yes;
  datarow   = 2;
  guessingrows = max;
run;

* Clean LinkedIn sector names;
data linkedin_clean;
  set linkedin_raw(keep=sector);
  sector_u = upcase(strip(sector));
  drop sector;
run;

* Macro to import and clean BLS data;
%macro import_and_clean(year);
  proc import datafile="&bls_path/natsector_M&year._dl.xlsx" out=raw_bls&year dbms=xlsx replace;
    getnames=yes;
  run;

  data bls&year;
    length NAICS $5 OCC_CODE $7 NAICS_TITLE $200;
    set raw_bls&year(rename=(TOT_EMP=TOT_EMP_char A_MEDIAN=A_MEDIAN_char));
    Year     = &year;
    TOT_EMP  = input(compress(TOT_EMP_char,,"kd"), best12.);
    A_MEDIAN = input(compress(A_MEDIAN_char,,"kd"), best12.);
    keep Year NAICS NAICS_TITLE OCC_CODE TOT_EMP A_MEDIAN;
  run;
%mend import_and_clean;

%import_and_clean(2022);
%import_and_clean(2023);
%import_and_clean(2024);

* Combine and filter BLS data;
data bls_all; set bls2022 bls2023 bls2024; run;
data bls_totals; set bls_all; where OCC_CODE = "00-0000"; run;

* Import and prep sector mapping;
proc import datafile="&mapping_csv" out=sector_map_raw dbms=csv replace;
  getnames=yes;
  guessingrows=max;
run;

data sector_map;
  set sector_map_raw(rename=(NAICS=NAICS_num sector=sector_raw));
  sector_u = upcase(strip(sector_raw));
  NAICS = put(NAICS_num, z2.);
  keep sector_u NAICS;
run;

* Count AI job postings by sector and map;
proc sql;
  create table ai_by_sector as
  select sector_u, count(*) as postings
  from linkedin_clean
  where sector_u ne ""
  group by sector_u;

  create table ai_by_naics as
  select m.NAICS, sum(s.postings) as AI_Postings_By_NAICS
  from ai_by_sector s
    inner join sector_map m on index(s.sector_u, m.sector_u) > 0
  group by m.NAICS;
quit;

* Merge BLS with AI data;
proc sql;
  create table bls_naics_merged as
  select B.Year, B.NAICS, B.NAICS_TITLE, B.TOT_EMP as Total_Employment,
         B.A_MEDIAN as Avg_Median_Wage,
         coalesce(N.AI_Postings_By_NAICS, 0) as AI_Postings_By_Sector
  from bls_totals B
  left join ai_by_naics N on B.NAICS = N.NAICS;
quit;

* Calculate job loss percentage and AI intensity;
proc sql;
  create table naics_analysis as
  select curr.NAICS, curr.Year as Year_Curr, prev.Year as Year_Prev,
         prev.Total_Employment as Emp_Prior, curr.Total_Employment as Emp_Curr,
         (prev.Total_Employment - curr.Total_Employment) / prev.Total_Employment as Job_Loss_Pct format=percent8.2,
         prev.AI_Postings_By_Sector,
         prev.AI_Postings_By_Sector / prev.Total_Employment as AI_Intensity format=8.4
  from bls_naics_merged curr
  inner join bls_naics_merged prev
    on curr.NAICS = prev.NAICS and curr.Year = prev.Year + 1;
quit;

* Add moderators;
proc sql;
  create table naics_analysis2 as
  select A.*, B.Total_Employment as Industry_Size, B.Avg_Median_Wage as Avg_Wage
  from naics_analysis A
  left join bls_naics_merged B on A.NAICS = B.NAICS and A.Year_Curr = B.Year;
quit;

* Center variables;
proc means data=naics_analysis2 noprint;
  var AI_Intensity Emp_Prior Avg_Wage;
  output out=_means_ mean=mu_AI mu_Emp mu_Wage;
run;

data naics_centered;
  if _n_=1 then set _means_;
  set naics_analysis2;
  cAI      = AI_Intensity - mu_AI;
  cEmp     = Emp_Prior - mu_Emp;
  cWage    = Avg_Wage - mu_Wage;
  cAI1000  = cAI * 1000;
  Intx_Emp = cAI1000 * cEmp;
  Intx_Wage= cAI1000 * cWage;
run;

* Transformed model (log of Job_Loss_Pct);
data naics_transformed;
  set naics_centered;
  shift = abs(min(Job_Loss_Pct)) + 0.01;
  LogJL = log(Job_Loss_Pct + shift);
run;

* Robust regression;
ods output Diagnostics=mm_diag;

proc robustreg data=naics_centered method=mm;
  model Job_Loss_Pct = cAI1000 cEmp cWage Intx_Emp Intx_Wage / diagnostics leverage;
  title "Robust Regression using MM Estimation";
run;

ods output close;

proc contents data=mm_diag;
run;

data mm_diag_scaled;
  set mm_diag;
  Leverage_Scaled = Leverage / max(Leverage);
run;

proc sgplot data=mm_diag_scaled;
  scatter x=Leverage_Scaled y=RResidual;
  refline 0 / axis=y lineattrs=(pattern=shortdash);
  xaxis label="Relative Leverage (Scaled 0–1)";
  yaxis label="Standardized Robust Residual";
  title "Figure 6. Residuals vs. Scaled Leverage (MM Estimation)";
run;




* Linear regression on log-transformed outcome;
proc reg data=naics_transformed;
  model LogJL = cAI1000 cEmp cWage Intx_Emp Intx_Wage;
  output out=log_reg_out p=PredLog r=ResidLog;
  title "Linear Regression on Log-Transformed Job Loss %";
run;

* Residual diagnostic plots;
proc sgplot data=log_reg_out;
  scatter x=PredLog y=ResidLog;
  refline 0 / axis=y lineattrs=(pattern=shortdash);
  title "Residuals vs Predicted (Log Model)";
run;

data variable_definitions;
  length Variable $25 Description $150;
  input Variable $ Description & $150.;
  datalines;
AI_Intensity     Proportion of AI-related job postings relative to total employment in the industry.
Job_Loss_Pct     Percent change in total industry employment between 2022 and 2024.
cAI1000          Centered AI job intensity (mean-centered to reduce multicollinearity).
cEmp             Centered total employment (industry size).
cWage            Centered average industry wage.
Intx_Emp         Interaction term: AI_Intensity × Employment.
Intx_Wage        Interaction term: AI_Intensity × Wage.
LogJL            Log-transformed Job Loss Percentage to stabilize variance.
;
run;

proc print data=variable_definitions noobs label;
  title "Table: Engineered Variable Definitions";
run;
