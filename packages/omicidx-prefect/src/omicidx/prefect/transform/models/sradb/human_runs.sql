MODEL (
  name sradb.human_runs,
  kind VIEW
);

SELECT * FROM sradb.run_with_study WHERE taxon_id = 9606;
