import unittest
from consumer import convertOwner
from consumer import parseRequest
from consumer import updateWidget


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

    def test_updateRequest(self):
        widgetToUpdate = {'name' : 'Michael', 'owner': 'Michael', 'height' : 12, 'age':21}
        whatToUpdate = {'name' : 'Michael2', 'owner': 'Michael2', 'age':22, 'year': 'senior'}
        self.assertEqual(updateWidget(widgetToUpdate, whatToUpdate), {'name': 'Michael2', 'owner': 'Michael', 'age' : 22, 'height':12, 'year':'senior'})
        self.assertNotEqual(updateWidget(widgetToUpdate, whatToUpdate), {'name': 'Michael2', 'owner': 'Michael2', 'age' : 22, 'height':12, 'year':'senior'})
        
        

    


if __name__ == '__main__': 
    unittest.main() 