from rectangle_area import rectangle_area_calculate
'''import the unittest module'''
import unittest

class DemoTest(unittest.TestCase):
    """ A New class called DemoTest is created that will inherit another class called TestCase from Unit Test"""
    def test(self):
        #self.assertTrue(5 > 1) #OK  #AssertTrue will return True if the expression is pass
        #self.assertTrue(5 < 1) #FAILED  #AssertTrue will return True if the expression is pass
        #self.assertEqual('D'*5,'DDDDD') #OK #AssertEqual will return True if the expression is pass
        #self.assertEqual('D' * 5, 'DDDD')  #FAILED
        self.assertEqual(rectangle_area_calculate(2,3),6)

    def test_l_gw_zero(self):
        self.assertGreater(rectangle_area_calculate(3,-2),0)

if __name__=="__main__":
    unittest.main()