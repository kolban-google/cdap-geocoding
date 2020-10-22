# Geocoding Transform

Description
-----------

Lookup an address that is contained within a named field in the input record using Google Maps Geocoding.  The
result is stored as JSON string in a named field in the output record.

Use Case
--------

This transform is used whenever you need to perform geocoding on an address.  This plugin encapsulates the [Google Maps
Geocoding API](https://developers.google.com/maps/documentation/geocoding/start).

Properties
----------

**apiKey:** The value of the API key used to access Google maps.

**addressFieldName:** The name of the field in the input record which contains the address be used to perform geocoding.  The field should be a String.

**geocodingFieldName:** The name of the field in the output record which will be populated with the returned record.  The output schema will be
augmented with a record of the following structure:

* `formattedAddress` - String - The full address returned from Google Maps.
* `geometry` - Record - Geometry information.
  * `latlng` - Record - Geo positioning information.
    * `lat` - Double - Lattitude.
    * `lng` - Double - Longitude.

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
