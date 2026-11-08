import unittest
from engine import Rules,calculate
from incremental import recalculate,award_key

def game(i,date,home,away,hs,away_score,ot=False):
    return dict(id=str(i),season=2025,date=date,home=home,away=away,hs=hs,as_=away_score,ot=ot)

GAMES=[
    game(1,"2025-09-01","A","B",24,17),
    game(2,"2025-09-08","B","C",28,14),
    game(3,"2025-09-15","C","A",17,20,True),
    game(4,"2025-09-22","A","D",21,7),
    game(5,"2025-09-29","B","D",14,10),
    game(6,"2025-10-06","C","D",31,28),
    game(7,"2025-10-13","D","A",24,21),
]

class IncrementalTests(unittest.TestCase):
    def test_new_game_matches_full_rebuild(self):
        for n in range(1,len(GAMES)):
            previous=calculate(GAMES[:n],Rules(1.5,.5,3))
            result=recalculate(GAMES[:n+1],previous)
            self.assertEqual(result["ledger"],calculate(GAMES[:n+1],Rules(1.5,.5,3)))
            updated={award_key(x):x for x in previous}
            for k in result["deletes"]:updated.pop(k)
            for x in result["upserts"]:updated[award_key(x)]=x
            self.assertEqual(updated,{award_key(x):x for x in result["ledger"]})
    def test_idempotent(self):
        prior=calculate(GAMES,Rules(1.5,.5,3))
        result=recalculate(GAMES,prior)
        self.assertEqual(result["changed_awards"],0)
    def test_corrected_result_changes_existing_awards(self):
        old=calculate(GAMES,Rules(1.5,.5,3))
        corrected=[dict(g) for g in GAMES]
        corrected[1]["hs"],corrected[1]["as_"]=14,28
        result=recalculate(corrected,old)
        self.assertGreater(result["changed_awards"],0)
        self.assertEqual(result["ledger"],calculate(corrected,Rules(1.5,.5,3)))
    def test_cutoff_removes_later_awards(self):
        old=calculate(GAMES,Rules(1.5,.5,3))
        result=recalculate(GAMES,old,as_of="2025-09-15")
        self.assertEqual(len(result["deletes"]),len(old)-6)
        self.assertEqual(len(result["ledger"]),6)
    def test_overtime_is_half_merit(self):
        result=recalculate(GAMES,[])
        self.assertEqual([x["merits"] for x in result["ledger"] if x["outcome"]=="OT L"],[.5])

if __name__=="__main__":unittest.main()
