WITH

  -- Changes in the lines of code
    locs AS (SELECT

              everything.email as email,
              everything.app as app,
              lower(SUBSTRING(everything.email, (position('@' in everything.email) + 1),
                              2500))                                          as domain,

              lower(SUBSTRING(everything.app, 0, (position('-' in
                                                           everything.app)))) as organization,
               everything.csha                                                        AS csha,
               everything.cwhen as cwhen,
               everything.message as message,

               COALESCE(metric.cnt, NULL) AS cnt
             FROM (SELECT DISTINCT
                     cs.csha as csha,
                     cs.email as email,
                     cs.application as app,
                     cs.cwhen as cwhen,
                     cs.message as message
                   FROM commits as cs

                  ) AS everything
               LEFT JOIN

               (select
                  cs.csha                                                        AS csha,
                  1 AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.ncloc <> mprev.ncloc
 						 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    locs_inc AS (SELECT
               everything.csha                                                        AS csha,

               COALESCE(metric.cnt, 0) AS cnt
             FROM (SELECT DISTINCT
                     csha
                   FROM commits

                  ) AS everything
               LEFT JOIN

               (SELECT
                  cs.csha                                                        AS csha,
                  1 AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.ncloc > mprev.ncloc
					 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    locs_dec AS (SELECT
               everything.csha                                                        AS csha,

               COALESCE(metric.cnt, 0) AS cnt
             FROM (SELECT DISTINCT
                     csha
                   FROM commits

                  ) AS everything
               LEFT JOIN

               (SELECT
                  cs.csha                                                        AS csha,
                  1 AS cnt
                FROM sonarqube_system_uni AS mcurr,
                  sonarqube_system_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.ncloc < mprev.ncloc
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    cmnts AS (SELECT
                everything.csha,
                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                      FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
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

                       ) AS metric
                  ON metric.csha = everything.csha),
    cmnts_inc AS (SELECT
                everything.csha,
                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                      FROM commits

                   ) AS everything
                LEFT JOIN
                (select DISTINCT
                   cs.csha          AS csha,
                   1 AS cnt
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
                       ) AS metric
                  ON metric.csha = everything.csha),
    cmnts_dec AS (SELECT
                everything.csha,
                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                      FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
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

                       ) AS metric
                  ON metric.csha = everything.csha),
    funcs AS (SELECT
                everything.csha,

                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                    FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.functions <> mprev.functions
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
                  ON metric.csha = everything.csha),
    funcs_inc AS (SELECT
                everything.csha,

                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                    FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.functions > mprev.functions
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
                  ON metric.csha = everything.csha),
    funcs_dec as (SELECT
                everything.csha,

                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                    FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.functions < mprev.functions
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
                  ON metric.csha = everything.csha),
    clss AS (SELECT
               everything.csha,

               COALESCE(metric.cnt, 0) AS cnt
             FROM (SELECT DISTINCT
                     csha
                   FROM commits

                  ) AS everything
               LEFT JOIN
               (SELECT
                  cs.csha          AS csha,
                  1 AS cnt
                FROM findbugs_summary_uni AS mcurr,
                  findbugs_summary_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.total_classes <> mprev.total_classes
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    clss_inc AS (SELECT
               everything.csha,

               COALESCE(metric.cnt, 0) AS cnt
             FROM (SELECT DISTINCT
                     csha
                   FROM commits

                  ) AS everything
               LEFT JOIN
               (SELECT
                  cs.csha          AS csha,
                  1 AS cnt
                FROM findbugs_summary_uni AS mcurr,
                  findbugs_summary_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.total_classes > mprev.total_classes
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    clss_dec AS (SELECT
               everything.csha,

               COALESCE(metric.cnt, 0) AS cnt
             FROM (SELECT DISTINCT
                     csha
                   FROM commits

                  ) AS everything
               LEFT JOIN
               (SELECT
                  cs.csha          AS csha,
                  1 AS cnt
                FROM findbugs_summary_uni AS mcurr,
                  findbugs_summary_uni AS mprev,
                  impact_pairs AS ip,
                  commits AS cs
                WHERE mcurr.total_classes < mprev.total_classes
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                      ) AS metric
                 ON metric.csha = everything.csha),
    pkgs AS
    (SELECT
       everything.csha,

       COALESCE(metric.cnt, 0) AS cnt
     FROM (SELECT DISTINCT
             csha
           FROM commits

          ) AS everything
       LEFT JOIN (SELECT
                    cs.csha          AS csha,

                    1 AS cnt
                  FROM findbugs_summary_uni AS mcurr,
                    findbugs_summary_uni AS mprev,
                    impact_pairs AS ip,
                    commits AS cs
                  WHERE mcurr.num_packages <> mprev.num_packages
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                        ) AS metric
         ON metric.csha = everything.csha
    ),
    pkgs_inc AS
    (SELECT
       everything.csha,

       COALESCE(metric.cnt, 0) AS cnt
     FROM (SELECT DISTINCT
             csha
           FROM commits

          ) AS everything
       LEFT JOIN (SELECT
                    cs.csha          AS csha,

                    1 AS cnt
                  FROM findbugs_summary_uni AS mcurr,
                    findbugs_summary_uni AS mprev,
                    impact_pairs AS ip,
                    commits AS cs
                  WHERE mcurr.num_packages > mprev.num_packages
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                        ) AS metric
         ON metric.csha = everything.csha
    ),
    pkgs_dec AS
    (SELECT
       everything.csha,

       COALESCE(metric.cnt, 0) AS cnt
     FROM (SELECT DISTINCT
             csha
           FROM commits

          ) AS everything
       LEFT JOIN (SELECT
                    cs.csha          AS csha,

                    1 AS cnt
                  FROM findbugs_summary_uni AS mcurr,
                    findbugs_summary_uni AS mprev,
                    impact_pairs AS ip,
                    commits AS cs
                  WHERE mcurr.num_packages < mprev.num_packages
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                        ) AS metric
         ON metric.csha = everything.csha
    ),
    cplxs AS (SELECT
                everything.csha,

                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                    FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.complexity <> mprev.complexity
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
                  ON metric.csha = everything.csha),
    cplxs_inc AS (SELECT
                everything.csha,

                COALESCE(metric.cnt, 0) AS cnt
              FROM (SELECT DISTINCT
                      csha
                    FROM commits

                   ) AS everything
                LEFT JOIN
                (SELECT
                   cs.csha          AS csha,
                   1 AS cnt
                 FROM sonarqube_system_uni AS mcurr,
                   sonarqube_system_uni AS mprev,
                   impact_pairs AS ip,
                   commits AS cs
                 WHERE mcurr.complexity > mprev.complexity
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
                  ON metric.csha = everything.csha),
    cplxs_dec AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                     WHERE mcurr.complexity < mprev.complexity
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                           ) AS metric
                      ON metric.csha = everything.csha),
    smls AS (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                           WHERE mcurr.code_smells <> mprev.code_smells
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                                 ) AS metric
              ON metric.csha = everything.csha),
    smls_inc AS (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                               WHERE mcurr.code_smells > mprev.code_smells
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                                     ) AS metric
                      ON metric.csha = everything.csha),
    smls_dec AS (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                               WHERE mcurr.code_smells < mprev.code_smells
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                                     ) AS metric
                      ON metric.csha = everything.csha),
    pmds AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric  ON metric.csha = everything.csha),
    pmds_inc AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric
      ON metric.csha = everything.csha),
    pmds_dec AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric
          ON metric.csha = everything.csha),
    vuls AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                 WHERE mcurr.vulnerabilities <> mprev.vulnerabilities
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
        ON metric.csha = everything.csha),
    vuls_inc AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                 WHERE mcurr.vulnerabilities > mprev.vulnerabilities
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
        ON metric.csha = everything.csha),
    vuls_dec AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
                     FROM sonarqube_system_uni AS mcurr,
                       sonarqube_system_uni AS mprev,
                       impact_pairs AS ip,
                       commits AS cs
                 WHERE mcurr.vulnerabilities < mprev.vulnerabilities
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                       ) AS metric
        ON metric.csha = everything.csha),
    scgs AS
    ( SELECT
        everything.csha,
        COALESCE(metric.cnt, 0) AS cnt
      FROM (SELECT DISTINCT
              csha
            FROM commits

           ) AS everything
        LEFT JOIN (SELECT
                     cs.csha          AS csha,
                     1 AS cnt
                   FROM pmd_uni AS mcurr,
                     pmd_uni AS mprev,
                     impact_pairs AS ip,
                     commits AS cs
                   WHERE
                     mcurr.securitycodeguidelines <> mprev.securitycodeguidelines
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                     ) AS metric
          ON metric.csha = everything.csha),
    scgs_inc AS
    ( SELECT
        everything.csha,
        COALESCE(metric.cnt, 0) AS cnt
      FROM (SELECT DISTINCT
              csha
            FROM commits

           ) AS everything
        LEFT JOIN (SELECT
                     cs.csha          AS csha,
                     1 AS cnt
                   FROM pmd_uni AS mcurr,
                     pmd_uni AS mprev,
                     impact_pairs AS ip,
                     commits AS cs
                 WHERE
                   mcurr.securitycodeguidelines > mprev.securitycodeguidelines
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                   ) AS metric
          ON metric.csha = everything.csha),
    scgs_dec AS
    ( SELECT
        everything.csha,
        COALESCE(metric.cnt, 0) AS cnt
      FROM (SELECT DISTINCT
              csha
            FROM commits

           ) AS everything
        LEFT JOIN (SELECT
                     cs.csha          AS csha,
                     1 AS cnt
                   FROM pmd_uni AS mcurr,
                     pmd_uni AS mprev,
                     impact_pairs AS ip,
                     commits AS cs
                 WHERE
                   mcurr.securitycodeguidelines < mprev.securitycodeguidelines
	 AND mcurr.csha = ip.curr
                      AND mprev.csha = ip.prev
                      AND cs.csha = mcurr.csha

                   ) AS metric
        ON metric.csha = everything.csha),
    fbgs AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric
          ON metric.csha = everything.csha),
    fbgs_inc AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, 0) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric
        ON metric.csha = everything.csha),
    fbgs_dec AS
    (SELECT
                    everything.csha,

                    COALESCE(metric.cnt, NULL) AS cnt
                  FROM (SELECT DISTINCT
                          csha
                        FROM commits

                       ) AS everything
                    LEFT JOIN
                    (SELECT
                       cs.csha          AS csha,
                       1 AS cnt
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

                       ) AS metric
          ON metric.csha = everything.csha),
          ip as (
                                                       (select prev as csha
                                                        from impact_pairs)
                                                       union
                                                       (select curr as csha
                                                        from impact_pairs)
                                                     ),
    quality_metrics as (
      SELECT
        locs.email         AS email,
        locs.organization as organization,
        locs.domain       as domain,
        locs.app           as app,
        locs.cwhen as cwhen,
        locs.message as message,
        --email_app_group_by_impactful.domain as domain,
        --email_app_group_by_impactful.organization as organization,
        --email_app_group_by_impactful.c_analyzed as c_analyzed,
        --email_app_group_by_impactful.c_impactful as c_impactful,
        locs.cnt           AS loc,
        cplxs.cnt          AS cpx,
        smls.cnt           AS sml,
        vuls.cnt           AS vul,
        fbgs.cnt           AS fbg,
        locs_inc.cnt       AS locs_inc,
        cplxs_inc.cnt      AS cplxs_inc,
        smls_inc.cnt       AS smls_inc,
        vuls_inc.cnt       AS vuls_inc,
        fbgs_inc.cnt       AS fbgs_inc,
        locs_dec.cnt       AS locs_dec,
        cplxs_dec.cnt      AS cplxs_dec,
        smls_dec.cnt       AS smls_dec,
        vuls_dec.cnt       AS vuls_dec,
        fbgs_dec.cnt       AS fbgs_dec
      FROM
        locs
        JOIN cmnts ON cmnts.csha = locs.csha
        JOIN funcs ON funcs.csha = cmnts.csha
        JOIN clss ON clss.csha = funcs.csha
        JOIN pkgs ON pkgs.csha = clss.csha
        JOIN cplxs ON cplxs.csha = pkgs.csha
        JOIN smls ON smls.csha = cplxs.csha
        JOIN pmds ON pmds.csha = smls.csha
        JOIN vuls ON vuls.csha = pmds.csha
        JOIN scgs ON scgs.csha = vuls.csha
        JOIN fbgs ON fbgs.csha = scgs.csha
        JOIN locs_dec ON locs_dec.csha = fbgs.csha
        JOIN cmnts_dec ON cmnts_dec.csha = locs_dec.csha
        JOIN funcs_dec ON funcs_dec.csha = cmnts_dec.csha
        JOIN clss_dec ON clss_dec.csha = funcs_dec.csha
        JOIN pkgs_dec ON pkgs_dec.csha = clss_dec.csha
        JOIN cplxs_dec ON cplxs_dec.csha = pkgs_dec.csha
        JOIN smls_dec ON smfbgs_incls_dec.csha = cplxs_dec.csha
        JOIN pmds_dec ON pmds_dec.csha = smls_dec.csha
        JOIN vuls_dec ON vuls_dec.csha = pmds_dec.csha
        JOIN scgs_dec ON scgs_dec.csha = vuls_dec.csha
        JOIN fbgs_dec ON fbgs_dec.csha = scgs_dec.csha
        JOIN locs_inc ON locs_inc.csha = fbgs_dec.csha
        JOIN cmnts_inc ON cmnts_inc.csha = locs_inc.csha
        JOIN funcs_inc ON funcs_inc.csha = cmnts_inc.csha
        JOIN clss_inc ON clss_inc.csha = funcs_inc.csha
        JOIN pkgs_inc ON pkgs_inc.csha = clss_inc.csha
        JOIN cplxs_inc ON cplxs_inc.csha = pkgs_inc.csha
        JOIN smls_inc ON smls_inc.csha = cplxs_inc.csha
        JOIN pmds_inc ON pmds_inc.csha = smls_inc.csha
        JOIN vuls_inc ON pmds_inc.csha = vuls_inc.csha
        JOIN scgs_inc ON scgs_inc.csha = vuls_inc.csha
        JOIN fbgs_inc ON fbgs_inc.csha = scgs_inc.csha
        RIGHT OUTER JOIN anal ON fbgs_inc.csha=anal.csha
    -- JOIN email_app_group_by_impactful ON email_app_group_by_impactful.email = fbgs_inc.email
  )

SELECT
*

FROM quality_metrics
