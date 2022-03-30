SELECT
    ff.ipeds_id,
    ff.year,
    ff.total_revenues,
    ff.excess_transfers_back,
    ff.total_expenses,
    ff.fbs_conference,
    cd.conference_abb,
    sd.school,
    sd.school_abb
FROM `project.ncaa.finances_fact` AS ff,
     `project.ncaa.conference_dim` AS cd,
     `project.ncaa.school_dim` AS sd
WHERE ff.fbs_conference = cd.fbs_conference
  AND ff.ipeds_id = sd.ipeds_id;