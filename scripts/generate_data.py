"""
generate_data.py
-----------------
Creates two synthetic CSV files that mimic real college records:
  - students.csv : StudentID, Name, Branch, Semester, Attendance
  - marks.csv    : StudentID, Subject, Marks

This keeps the project self-contained (no external API/key needed) while
still looking and behaving like real institutional data.
"""

import csv
import random

random.seed(42)

BRANCHES = ["CSE", "ECE", "AI", "Cloud", "Mechanical"]
SUBJECTS = ["Cloud Computing", "DBMS", "DSA", "Operating Systems", "Networks"]
NUM_STUDENTS = 500  # small + fast, but enough to show real groupBy/join behaviour

names_first = ["Aarav","Vivaan","Aditya","Sai","Reyansh","Ishaan","Vihaan","Arjun",
               "Ananya","Diya","Kavya","Myra","Anika","Sara","Riya","Priya",
               "Yuvi","Rohan","Kabir","Zara","Meera","Neha","Aryan","Tara"]
names_last = ["Sharma","Verma","Gupta","Iyer","Nair","Reddy","Singh","Patel",
              "Mehta","Chauhan","Kapoor","Joshi","Rao","Das","Bose","Malik"]

def generate_students(n):
    rows = []
    for i in range(1, n + 1):
        sid = f"S{i:04d}"
        name = f"{random.choice(names_first)} {random.choice(names_last)}"
        branch = random.choice(BRANCHES)
        semester = random.randint(1, 8)
        # Attendance: most students healthy (70-99%), a realistic tail below 75%
        attendance = round(random.triangular(35, 99, 82), 1)
        rows.append([sid, name, branch, semester, attendance])
    return rows

def generate_marks(students):
    """Marks are loosely correlated with attendance (lower attendance -> lower
    average marks, with noise) so the at-risk rule surfaces a realistic ~12-18%
    of students, instead of an almost-empty result."""
    rows = []
    for sid, attendance in students:
        subs = random.sample(SUBJECTS, k=random.randint(3, 5))
        # base ability loosely tracks attendance (chronic absentees trend lower),
        # with enough noise that it's a tendency, not a deterministic rule —
        # surfaces a realistic ~15-18% at-risk rate, not everyone below the line
        ability_center = 35 + (attendance - 40) * 0.4
        for sub in subs:
            noise = random.triangular(-25, 25, 0)
            mark = max(2, min(100, ability_center + noise))
            rows.append([sid, sub, round(mark, 1)])
    return rows

def main():
    students = generate_students(NUM_STUDENTS)
    student_attendance_pairs = [(r[0], r[4]) for r in students]
    marks = generate_marks(student_attendance_pairs)

    with open("data/students.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["StudentID", "Name", "Branch", "Semester", "Attendance"])
        w.writerows(students)

    with open("data/marks.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["StudentID", "Subject", "Marks"])
        w.writerows(marks)

    print(f"Generated {len(students)} students and {len(marks)} marks records.")

if __name__ == "__main__":
    main()
