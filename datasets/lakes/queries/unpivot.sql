SELECT * EXCEPT(all_lakes) FROM `project.lakes.Sheet1`
UNPIVOT
(ice_coverage for lake in (superior, michigan, huron, erie, ontario))