// Jamel Peralta -> Hive sql
// part 1
select e.region, e.municipio, count(*) from students as s, escuelapr as e where s.sid = e.eid group by e.region, e.municipio;
// part 2
select municipio, nivel, count(*) from escuelapr group by municipio, nivel;
// part 3
select count(*) from students as s, escuelapr as e where e.eid = s.sid and e.municipio = "Ponce" and e.nivel = "Superior" and s.sexo = "F";
// part 4
select e.region, e.distrito, e.municipio, count(*) from students as s, escuelapr as e where s.sid = e.eid and s.sexo = "M" group by e.region, e.distrito, e.municipio;
