MODEL (
  name sradb.mouse_runs,
  kind VIEW
);

SELECT * FROM sradb.run_with_study WHERE taxon_id = 10090;
