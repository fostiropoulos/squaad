with
everything as (
select
	distinct csha, application, cwhen, message, branch, email
from
	commits ) ,
pmd as (
select
	mcurr.csha as csha,
	mcurr.basic - mprev.basic as basic,
	mcurr.emptycode - mprev.emptycode as emptycode,
	mcurr.cloneimplementation - mprev.cloneimplementation as cloneimplementation,
	mcurr.comments - mprev.comments as comments,
	mcurr.codesize - mprev.codesize as codesize,
	mcurr.stringandstringbuffer - mprev.stringandstringbuffer as stringandstringbuffer,
	mcurr.naming - mprev.naming as naming,
	mcurr.strictexceptions - mprev.strictexceptions as strictexceptions,
	mcurr.optimization - mprev.optimization as optimization,
	mcurr.design - mprev.design as design,
	mcurr.securitycodeguidelines - mprev.securitycodeguidelines as securitycodeguidelines,
	mcurr.braces - mprev.braces as braces,
	mcurr.typeresolution - mprev.typeresolution as typeresolution,
	mcurr.coupling - mprev.coupling as coupling,
	mcurr.finalizer - mprev.finalizer as finalizer,
	mcurr.importstatements - mprev.importstatements as importstatements,
	mcurr.unusedcode - mprev.unusedcode as unusedcode,
	mcurr.unnecessary - mprev.unnecessary as unnecessary
from
	pmd_uni as mcurr,
	pmd_uni as mprev,
	impact_pairs as ip,
	commits as cs
where
	mcurr.csha = ip.curr
	and mprev.csha = ip.prev
	and cs.csha = mcurr.csha),
sonarqube as (
select
	mcurr.csha,
	mcurr.classes-mprev.classes as classes,
	mcurr.comment_lines_density-mprev.comment_lines_density as comment_lines_density,
	mcurr.vulnerabilities-mprev.vulnerabilities as vulnerabilities,
	mcurr.lines-mprev.lines as lines,
	mcurr.ncloc-mprev.ncloc as ncloc,
	mcurr.complexity-mprev.complexity as complexity,
	mcurr.security_rating-mprev.security_rating as security_rating,
	mcurr.major_violations-mprev.major_violations as major_violations,
	mcurr.duplicated_blocks-mprev.duplicated_blocks as duplicated_blocks,
	mcurr.code_smells-mprev.code_smells as code_smells,
	mcurr.file_complexity-mprev.file_complexity as file_complexity,
	mcurr.functions-mprev.functions as functions,
	mcurr.duplicated_files-mprev.duplicated_files as duplicated_files,
	mcurr.duplicated_lines_density-mprev.duplicated_lines_density as duplicated_lines_density,
	mcurr.reliability_rating-mprev.reliability_rating as reliability_rating,
	mcurr.critical_violations-mprev.critical_violations as critical_violations,
	mcurr.violations-mprev.violations as violations,
	mcurr.statements-mprev.statements as statements,
	mcurr.blocker_violations-mprev.blocker_violations as blocker_violations,
	mcurr.reliability_remediation_effort-mprev.reliability_remediation_effort as reliability_remediation_effort,
	mcurr.duplicated_lines-mprev.duplicated_lines as duplicated_lines,
	mcurr.bugs-mprev.bugs as bugs,
	mcurr.security_remediation_effort-mprev.security_remediation_effort as security_remediation_effort,
	mcurr.directories-mprev.directories as directories,
	mcurr.info_violations-mprev.info_violations as info_violations,
	mcurr.sqale_index-mprev.sqale_index as sqale_index,
	mcurr.sqale_debt_ratio-mprev.sqale_debt_ratio as sqale_debt_ratio,
	mcurr.minor_violations-mprev.minor_violations as minor_violations,
	mcurr.files-mprev.files as files,
	mcurr.sqale_rating-mprev.sqale_rating as sqale_rating
from
	sonarqube_system_uni as mcurr,
	sonarqube_system_uni as mprev,
	impact_pairs as ip,
	commits as cs
where
	mcurr.csha = ip.curr
	and mprev.csha = ip.prev
	and cs.csha = mcurr.csha),
findbugs as (
select
	mcurr.csha,
	mcurr.security-mprev.security as security,
	mcurr.bad_practice-mprev.bad_practice as bad_practice,
	mcurr.malicious_code-mprev.malicious_code as malicious_code,
	mcurr.performance-mprev.performance as performance,
	mcurr.correctness-mprev.correctness as correctness,
	mcurr.style-mprev.style as style,
	mcurr.experimental-mprev.experimental as experimental,
	mcurr.mt_correctness-mprev.mt_correctness as mt_correctness,
	mcurr.i18n-mprev.i18n as i18n
from
	findbugs_type_category_uni as mcurr,
	findbugs_type_category_uni as mprev,
	impact_pairs as ip,
	commits as cs
where
	mcurr.csha = ip.curr
	and mprev.csha = ip.prev
	and cs.csha = mcurr.csha)

select
	*

from
	everything
left join sonarqube on
	everything.csha = sonarqube.csha
left join pmd on
	everything.csha = pmd.csha
left join findbugs on
	everything.csha = findbugs.csha
