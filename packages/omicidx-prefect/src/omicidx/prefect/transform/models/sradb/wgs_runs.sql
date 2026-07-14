MODEL (
  name sradb.wgs_runs,
  kind VIEW
);

SELECT * FROM sradb.run_with_study WHERE library_strategy = 'WGS';
