import unittest
from consumer import convertOwner
from consumer import parseRequest

class Test(unittest.TestCase):
    def test_convertOwner(self):
        self.assertEqual(convertOwner("Sue Smith"), "sue-smith")
        self.assertEqual(convertOwner("John Harold"), "john-harold")
        self.assertEqual(convertOwner("MichAeL ParKEr"), "michael-parker")
        self.assertNotEqual(convertOwner("MichAeL ParKEr"), "michaelparker")
        self.assertNotEqual(convertOwner("Michael   Parker"), "michael-parker")



    def test_parseRequest(self):
        givenRequest = {
            'otherAttributes':[{'name': 'name', 'value': 'michael'},
                               {'name': 'sizeType', 'value': 'feet'},
                               {'name': 'height', 'value': 6}
                               ]}
        givenWrongRequest = {'otasdfasdfbutes':[{'name': 'name', 'value': 'michael'},
                               {'name': 'sizeType', 'value': 'feet'},
                               {'name': 'height', 'value': 6}
                               ]}
        
        self.assertEqual(parseRequest(givenRequest), {'name': 'michael', 'sizeType': 'feet', 'height': 6})
        self.assertEqual(parseRequest(givenWrongRequest), givenWrongRequest)

    


if __name__ == '__main__': 
    unittest.main() 