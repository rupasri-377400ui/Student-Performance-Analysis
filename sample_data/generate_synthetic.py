"""Generate SYNTHETIC test data (random, NOT real students) with the same columns
as the UCI student-mat.csv. For testing the pipeline only. Never report results
from this file as real findings."""
import csv, random
random.seed(42)
cols = ["school","sex","age","address","famsize","Pstatus","Medu","Fedu","Mjob","Fjob",
        "reason","guardian","traveltime","studytime","failures","schoolsup","famsup","paid",
        "activities","nursery","higher","internet","romantic","famrel","freetime","goout",
        "Dalc","Walc","health","absences","G1","G2","G3"]
def pick(x): return random.choice(x)
rows = []
for _ in range(395):
    st = random.randint(1, 4); fl = random.choices([0,1,2,3],[75,12,8,5])[0]
    ab = random.choices([0,2,4,6,8,10,14,20,30],[30,20,15,10,8,6,5,4,2])[0]
    g1 = max(0, min(20, round(random.gauss(10 + st*0.5 - fl, 3))))
    g2 = max(0, min(20, round(g1 + random.gauss(0, 1.5))))
    g3 = max(0, min(20, round(g2 + random.gauss(0, 1.5))))
    rows.append([pick(["GP","MS"]),pick(["F","M"]),random.randint(15,22),pick(["U","R"]),
        pick(["GT3","LE3"]),pick(["T","A"]),random.randint(0,4),random.randint(0,4),
        pick(["teacher","health","services","at_home","other"]),
        pick(["teacher","health","services","at_home","other"]),
        pick(["home","reputation","course","other"]),pick(["mother","father","other"]),
        random.randint(1,4),st,fl,pick(["yes","no"]),pick(["yes","no"]),pick(["yes","no"]),
        pick(["yes","no"]),pick(["yes","no"]),pick(["yes","no"]),pick(["yes","no"]),
        pick(["yes","no"]),random.randint(1,5),random.randint(1,5),random.randint(1,5),
        random.randint(1,5),random.randint(1,5),random.randint(1,5),ab,g1,g2,g3])
with open("sample_data/synthetic_student_mat.csv","w",newline="") as f:
    w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(cols); w.writerows(rows)
