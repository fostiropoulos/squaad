WITH email_app_group_by_impactful AS (select
                                        cs.email                                                        as email,
                                        cs.application                                                  as app,
                                        lower(SUBSTRING(cs.email, (position('@' in cs.email) + 1),
                                                        2500))                                          as domain,

                                        lower(SUBSTRING(cs.application, 0, (position('-' in
                                                                                     cs.application)))) as organization,
                                        max(
                                            analreal.impactful)                                         as c_impactful,
                                        max(
                                            analreal.analyzed)                                          as c_analyzed


                                      from
                                        commits as cs
                                        , (
                                            select
                                              cs.email                 as email,
                                              cs.application           as app,
                                              max(recovered.compiled)  as analyzed,
                                              max(recovered.cnt_email) as dev_analyazed,
                                              count(distinct ip.csha)  as impactful,
                                              count(distinct cs.email) as dev_impactful,
                                              max(recovered.maxwhen)   as maxwhen,
                                              min(recovered.minwhen)   as minwhen
                                            from
                                              (
                                                (select prev as csha
                                                 from impact_pairs)
                                                union
                                                (select curr as csha
                                                 from impact_pairs)
                                              )
                                                as ip
                                              , commits as cs
                                              , (
                                                  select
                                                    cs.email                  as email,
                                                    cs.application            as app,
                                                    count(distinct anal.csha) as compiled,
                                                    count(distinct cs.email)  as cnt_email,
                                                    min(cwhen)                as minwhen,
                                                    max(cwhen)                as maxwhen
                                                  from
                                                    (
                                                      (select prev as csha
                                                       from impact_pairs)
                                                      union
                                                      (select curr as csha
                                                       from impact_pairs)
                                                    )
                                                      as ip
                                                    , commits as cs
                                                    , (
                                                        (select
                                                           application,
                                                           csha
                                                         from findbugs_summary_uni)
                                                        union
                                                        (select
                                                           application,
                                                           csha
                                                         from pmd_uni)
                                                        union
                                                        (select
                                                           application,
                                                           csha
                                                         from sonarqube_system_uni)
                                                      )
                                                    as anal
                                                  where cs.csha = anal.csha
                                                        and cs.csha = ip.csha
                                                        and cs.application = anal.application
                                                  group by cs.email, cs.application

                                                ) as recovered
                                            where cs.csha = ip.csha
                                                  and cs.cwhen <= recovered.maxwhen
                                                  and cs.cwhen >= recovered.minwhen
                                                  and cs.application = recovered.app
                                                  and cs.email = recovered.email
                                            group by cs.email, cs.application
                                          ) as analreal
                                      where
                                        cs.application = analreal.app
                                        and cs.cwhen <= analreal.maxwhen
                                        and cs.cwhen >= analreal.minwhen
                                        and cs.email = analreal.email
                                      group by cs.email, cs.application
                                      order by cs.email, cs.application)


select * from email_app_group_by_impactful
