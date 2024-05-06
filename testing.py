import unittest
from io import StringIO
from main import  *

class TestDataValidator(unittest.TestCase):

    # Setup things when test file is invoked
    def setUp(self):
        self.output_csv = StringIO()

    # close and clean things after execution
    def tearDown(self):
        self.output_csv.close()

    def test_valid_row(self):
        # Test the valid_row function
        valid_row_data = {
        'Email': 'colton_tromp@gmail.com',
        'First Name': 'Darcy',
        'Last Name': 'Waters',
        'Residential Address Postcode': '8540',
        'Postal Address Postcode': '6315'
        }

        invalid_row_data = { 'Email': 'hjkhjk',
            'First Name': 'Darcy',  
            'Last Name': 'Waters',   
            'Residential Address Postcode': 'kjkljr',  
            'Postal Address Postcode': '54321'}
        
        assert valid_row(valid_row_data) == True
        assert valid_row(invalid_row_data) == False

    def test_geocode_address(self, tolerance=0.001):
        address = [("AMBLESIDE TAS 7310", (-37.939795000000004, 146.4046230412041)),
               ("ARTHUR RIVER WA 6315", (-20.272494000000002, 148.71814657732955))]

        for address, expected_coordinates in address:
            try:
                geocoded_coordinates = geocode_address(address)
                assert abs(geocoded_coordinates[0] - expected_coordinates[0]) < tolerance
                assert abs(geocoded_coordinates[1] - expected_coordinates[1]) < tolerance
            except Exception as e:
                print(f"Geocoding failed for address: {address}. Error: {e}")




if __name__ == "__main__":
    unittest.main()
