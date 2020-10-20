package com.examples.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;

public class CDAPUtils {
    /**
     * Copy the input record to the output record ensuring that a field in the input exists in the output.
     * @param inputRecord The input record containing data.
     * @param outputSchema The schema of the output record.
     * @param outputBuilder THe builder to be populated with the input fields.
     */
    public static void copy(StructuredRecord inputRecord, Schema outputSchema, StructuredRecord.Builder outputBuilder) {
        Schema inputSchema = inputRecord.getSchema();
        for (Field field: inputSchema.getFields()) { // Iterate over each of the fields in the input.
            String fieldName = field.getName();  // Get the name of the curremt input field.
            Field f = outputSchema.getField(fieldName);  // Check that we have a field in the output that is the same name as the one in the input.
            System.out.println("Output field corresponding to " + fieldName + " is " + f);
            // If we don't have a field in the output schema of the same name as the input schema then skip this field.
            if (f != null) {
                Object o = inputRecord.get(fieldName); // Get the value of the current input field.
                outputBuilder.set(fieldName, o); // Set the value in the output field.
            }
        } // For each field.
    } // copy
}
