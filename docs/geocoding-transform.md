# Geocoding Transform

Description
-----------

Lookup an address that is contained within a named field in the input record using Google Maps Geocoding.  The
result is stored as JSON string in a named field in the output record.

Use Case
--------

This transform is used whenever you need to perform geocoding on an address.

Properties
----------

**apiKey:** The value of the API key used to access Google maps.

**addressFieldName:** The name of the field in the input record which will be used to perform geocoding.

**geocodingFieldName:** The name of the field in the output record which will be populated with the JSON string representing the geocoding result.

Example
-------

This example looks up the address contained in the `address` input field and stores the geocoding result in the field called `result` in the output record.

    {
        "name": "geocoding",
        "type": "transform",
        "properties": {
            "apiKey": "XYZ123....",
            "addressFieldName": "address",
            "geocodingFieldName": "result"
        }
    }
