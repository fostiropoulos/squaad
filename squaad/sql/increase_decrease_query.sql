WITH
                                      total AS
(SELECT
   cs.email          AS email,
   cs.application    AS app,
   Count(mcurr.csha) AS cnt
 FROM sonarqube_system_uni AS mcurr,
   sonarqube_system_uni AS mprev,
   impact_pairs AS ip,
   commits AS cs
 WHERE mcurr.csha = ip.curr
       AND mprev.csha = ip.prev
       AND cs.csha = mcurr.csha

 GROUP BY cs.email, cs.application),
  -- Changes in the lines of code
    locs AS

  (SELECT
     cs.email          AS email,

     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.ncloc <> mprev.ncloc
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    locs_inc AS

  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.ncloc > mprev.ncloc
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    locs_dec AS

  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.ncloc < mprev.ncloc
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    cmnts AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE Abs(mcurr.ncloc * mcurr.comment_lines_density /
             (100 - mcurr.comment_lines_density) -
             mprev.ncloc * mprev.comment_lines_density /
             (100 - mprev.comment_lines_density)) >= 1
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    cmnts_inc AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE (mcurr.ncloc * mcurr.comment_lines_density /
          (100 - mcurr.comment_lines_density) -
          mprev.ncloc * mprev.comment_lines_density /
          (100 - mprev.comment_lines_density)) >= 1
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    cmnts_dec AS
  (SELECT
     cs.email          AS email,

     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE (mcurr.ncloc * mcurr.comment_lines_density /
          (100 - mcurr.comment_lines_density) -
          mprev.ncloc * mprev.comment_lines_density /
          (100 - mprev.comment_lines_density)) <= -1
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    funcs AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.functions <> mprev.functions
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    funcs_inc AS
  (SELECT
     cs.email          AS email,

     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.functions > mprev.functions
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    funcs_dec AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.functions < mprev.functions
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    clss AS
  (SELECT
     cs.email          AS email,

     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM findbugs_summary_uni AS mcurr,
     findbugs_summary_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.total_classes <> mprev.total_classes
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    clss_inc AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM findbugs_summary_uni AS mcurr,
     findbugs_summary_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.total_classes > mprev.total_classes
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    clss_dec AS
  (SELECT
     cs.email          AS email,
     cs.application    AS app,
     Count(mcurr.csha) AS cnt
   FROM findbugs_summary_uni AS mcurr,
     findbugs_summary_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.total_classes < mprev.total_classes
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    pkgs AS
  (SELECT
     everything.email,
     everything.application  as app,

     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    AS app,

                  Count(mcurr.csha) AS cnt
                FROM findbugs_summary_uni AS mcurr,
                  findbugs_summary_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.num_packages <> mprev.num_packages
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application
  ),
    pkgs_inc AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits


        ) AS everything
     LEFT JOIN (
                 SELECT
                   cs.email          AS email,
                   cs.application    AS app,
                   Count(mcurr.csha) AS cnt
                 FROM findbugs_summary_uni AS mcurr,
                   findbugs_summary_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.num_packages > mprev.num_packages
                       AND mcurr.csha = ip.curr
                       AND mprev.csha = ip.prev
                       AND cs.csha = mcurr.csha

                 GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    pkgs_dec AS
  (SELECT
     everything.email,
     everything.application  as app,

     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    AS app,

                  Count(mcurr.csha) AS cnt
                FROM findbugs_summary_uni AS mcurr,
                  findbugs_summary_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.num_packages < mprev.num_packages
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email

          and metric.app = everything.application),
    cplxs AS
  (SELECT
     cs.email          AS email,
     cs.application    as app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.complexity <> mprev.complexity
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    cplxs_inc AS
  (SELECT
     cs.email          AS email,
     cs.application    as app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.complexity > mprev.complexity
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    cplxs_dec AS
  (SELECT
     cs.email          AS email,
     cs.application    as app,
     Count(mcurr.csha) AS cnt
   FROM sonarqube_system_uni AS mcurr,
     sonarqube_system_uni AS mprev,
     impact_pairs AS ip,
     commits AS cs
   WHERE mcurr.complexity < mprev.complexity
         AND mcurr.csha = ip.curr
         AND mprev.csha = ip.prev
         AND cs.csha = mcurr.csha

   GROUP BY cs.email, cs.application),
    smls AS (SELECT
               cs.email          AS email,
               cs.application    as app,
               Count(mcurr.csha) AS cnt
             FROM sonarqube_system_uni AS mcurr,
               sonarqube_system_uni AS mprev,
               impact_pairs AS ip,
               commits AS cs
             WHERE mcurr.code_smells <> mprev.code_smells
                   AND mcurr.csha = ip.curr
                   AND mprev.csha = ip.prev
                   AND cs.csha = mcurr.csha

             GROUP BY cs.email, cs.application),
    smls_inc AS (SELECT
                   cs.email          AS email,
                   cs.application    as app,
                   Count(mcurr.csha) AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.code_smells > mprev.code_smells
                       AND mcurr.csha = ip.curr
                       AND mprev.csha = ip.prev
                       AND cs.csha = mcurr.csha

                 GROUP BY cs.email, cs.application),
    smls_dec AS (SELECT
                   cs.email          AS email,

                   cs.application    as app,
                   Count(mcurr.csha) AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.code_smells < mprev.code_smells
                       AND mcurr.csha = ip.curr
                       AND mprev.csha = ip.prev
                       AND cs.csha = mcurr.csha

                 GROUP BY cs.email, cs.application),
    pmds AS
  (SELECT
     everything.email,
     everything.application  as app,

     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE Abs((
                            mcurr.emptycode + mcurr.naming + mcurr.braces
                            + mcurr.importstatements + mcurr.coupling
                            + mcurr.unusedcode + mcurr.unnecessary
                            + mcurr.design + mcurr.optimization
                            + mcurr.stringandstringbuffer
                            + mcurr.codesize) - (
                            mprev.emptycode + mprev.naming + mprev.braces
                            + mprev.importstatements + mprev.coupling
                            + mprev.unusedcode + mprev.unnecessary
                            + mprev.design + mprev.optimization
                            + mprev.stringandstringbuffer
                            + mprev.codesize)) > 0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    pmds_inc AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE ((
                         mcurr.emptycode + mcurr.naming + mcurr.braces
                         + mcurr.importstatements + mcurr.coupling
                         + mcurr.unusedcode + mcurr.unnecessary
                         + mcurr.design + mcurr.optimization
                         + mcurr.stringandstringbuffer
                         + mcurr.codesize) - (
                         mprev.emptycode + mprev.naming + mprev.braces
                         + mprev.importstatements + mprev.coupling
                         + mprev.unusedcode + mprev.unnecessary
                         + mprev.design + mprev.optimization
                         + mprev.stringandstringbuffer
                         + mprev.codesize)) > 0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    pmds_dec AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE ((
                         mcurr.emptycode + mcurr.naming + mcurr.braces
                         + mcurr.importstatements + mcurr.coupling
                         + mcurr.unusedcode + mcurr.unnecessary
                         + mcurr.design + mcurr.optimization
                         + mcurr.stringandstringbuffer
                         + mcurr.codesize) - (
                         mprev.emptycode + mprev.naming + mprev.braces
                         + mprev.importstatements + mprev.coupling
                         + mprev.unusedcode + mprev.unnecessary
                         + mprev.design + mprev.optimization
                         + mprev.stringandstringbuffer
                         + mprev.codesize)) < 0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    vuls AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.vulnerabilities <> mprev.vulnerabilities
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    vuls_inc AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.vulnerabilities > mprev.vulnerabilities
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    vuls_dec AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.vulnerabilities < mprev.vulnerabilities
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    scgs AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE
                  mcurr.securitycodeguidelines <> mprev.securitycodeguidelines
                  AND mcurr.csha = ip.curr
                  AND mprev.csha = ip.prev
                  AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    scgs_inc AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE
                  mcurr.securitycodeguidelines > mprev.securitycodeguidelines
                  AND mcurr.csha = ip.curr
                  AND mprev.csha = ip.prev
                  AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    scgs_dec AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM pmd_uni AS mcurr,
                  pmd_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE
                  mcurr.securitycodeguidelines < mprev.securitycodeguidelines
                  AND mcurr.csha = ip.curr
                  AND mprev.csha = ip.prev
                  AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    fbgs AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM findbugs_type_category_uni AS mcurr,
                  findbugs_type_category_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE Abs((mcurr.security + mcurr.malicious_code)
                          - (
                            mprev.security + mprev.malicious_code)) >
                      0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    fbgs_inc AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

         ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM findbugs_type_category_uni AS mcurr,
                  findbugs_type_category_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE ((mcurr.security + mcurr.malicious_code)
                       - (
                         mprev.security + mprev.malicious_code)) >
                      0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application),
    fbgs_dec AS
  (SELECT
     everything.email,
     everything.application  as app,
     COALESCE(metric.cnt, 0) AS cnt
   FROM (SELECT DISTINCT
           email,
           application
         FROM commits

        ) AS everything
     LEFT JOIN (SELECT
                  cs.email          AS email,
                  cs.application    as app,
                  Count(mcurr.csha) AS cnt
                FROM findbugs_type_category_uni AS mcurr,
                  findbugs_type_category_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE ((mcurr.security + mcurr.malicious_code)
                       - (
                         mprev.security + mprev.malicious_code)) <
                      0
                      AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                GROUP BY cs.email, cs.application) AS metric
       ON metric.email = everything.email
          and metric.app = everything.application
   order by email, app)

SELECT
  locs.email    AS email,
  locs.app      as app,
  --email_app_group_by_impactful.domain as domain,
  --email_app_group_by_impactful.organization as organization,
  --email_app_group_by_impactful.c_analyzed as c_analyzed,
  --email_app_group_by_impactful.c_impactful as c_impactful,
  locs.cnt      AS loc,
  cplxs.cnt     AS cpx,
  smls.cnt      AS sml,
  vuls.cnt      AS vul,
  fbgs.cnt      AS fbg,
  locs_inc.cnt  AS locs_inc,
  cplxs_inc.cnt AS cplxs_inc,
  smls_inc.cnt  AS smls_inc,
  vuls_inc.cnt  AS vuls_inc,
  fbgs_inc.cnt  AS fbgs_inc,
  locs_dec.cnt  AS locs_dec,
  cplxs_dec.cnt AS cplxs_dec,
  smls_dec.cnt  AS smls_dec,
  vuls_dec.cnt  AS vuls_dec,
  fbgs_dec.cnt  AS fbgs_dec,
  total.cnt     AS total
FROM
  total
  JOIN locs ON locs.email = total.email AND locs.app = total.app
  JOIN cmnts ON cmnts.email = locs.email AND cmnts.app = locs.app
  JOIN funcs ON funcs.email = cmnts.email AND funcs.app = cmnts.app
  JOIN clss ON clss.email = funcs.email AND clss.app = funcs.app
  JOIN pkgs ON pkgs.email = clss.email AND pkgs.app = clss.app
  JOIN cplxs ON cplxs.email = pkgs.email AND cplxs.app = pkgs.app
  JOIN smls ON smls.email = cplxs.email AND smls.app = cplxs.app
  JOIN pmds ON pmds.email = smls.email AND pmds.app = smls.app
  JOIN vuls ON vuls.email = pmds.email AND vuls.app = pmds.app
  JOIN scgs ON scgs.email = vuls.email AND scgs.app = vuls.app
  JOIN fbgs ON fbgs.email = scgs.email AND fbgs.app = scgs.app
  JOIN locs_dec ON locs_dec.email = fbgs.email AND locs_dec.app = fbgs.app
  JOIN cmnts_dec ON cmnts_dec.email = locs_dec.email AND cmnts_dec.app = locs_dec.app
  JOIN funcs_dec ON funcs_dec.email = cmnts_dec.email AND funcs_dec.app = cmnts_dec.app
  JOIN clss_dec ON clss_dec.email = funcs_dec.email AND clss_dec.app = funcs_dec.app
  JOIN pkgs_dec ON pkgs_dec.email = clss_dec.email AND pkgs_dec.app = clss_dec.app
  JOIN cplxs_dec ON cplxs_dec.email = pkgs_dec.email AND cplxs_dec.app = pkgs_dec.app
  JOIN smls_dec ON smls_dec.email = cplxs_dec.email AND smls_dec.app = cplxs_dec.app
  JOIN pmds_dec ON pmds_dec.email = smls_dec.email AND pmds_dec.app = smls_dec.app
  JOIN vuls_dec ON vuls_dec.email = pmds_dec.email AND vuls_dec.app = pmds_dec.app
  JOIN scgs_dec ON scgs_dec.email = vuls_dec.email AND scgs_dec.app = vuls_dec.app
  JOIN fbgs_dec ON fbgs_dec.email = scgs_dec.email AND fbgs_dec.app = scgs_dec.app
  JOIN locs_inc ON locs_inc.email = fbgs_dec.email AND locs_inc.app = fbgs_dec.app
  JOIN cmnts_inc ON cmnts_inc.email = locs_inc.email AND cmnts_inc.app = locs_inc.app
  JOIN funcs_inc ON funcs_inc.email = cmnts_inc.email AND funcs_inc.app = cmnts_inc.app
  JOIN clss_inc ON clss_inc.email = funcs_inc.email AND clss_inc.app = funcs_inc.app
  JOIN pkgs_inc ON pkgs_inc.email = clss_inc.email AND pkgs_inc.app = clss_inc.app
  JOIN cplxs_inc ON cplxs_inc.email = pkgs_inc.email AND cplxs_inc.app = pkgs_inc.app
  JOIN smls_inc ON smls_inc.email = cplxs_inc.email AND smls_inc.app = cplxs_inc.app
  JOIN pmds_inc ON pmds_inc.email = smls_inc.email AND pmds_inc.app = smls_inc.app
  JOIN vuls_inc ON pmds_inc.email = vuls_inc.email AND pmds_inc.app = vuls_inc.app
  JOIN scgs_inc ON scgs_inc.email = vuls_inc.email AND scgs_inc.app = vuls_inc.app
  JOIN fbgs_inc ON fbgs_inc.email = scgs_inc.email AND fbgs_inc.app = scgs_inc.app
 -- JOIN email_app_group_by_impactful ON email_app_group_by_impactful.email = fbgs_inc.email AND email_app_group_by_impactful.app = fbgs_inc.app
