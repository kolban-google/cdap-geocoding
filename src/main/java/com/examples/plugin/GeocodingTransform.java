
package com.examples.plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.api.data.schema.Schema.Type;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("geocoding")
@Description("Transforms configured fields to lowercase or uppercase.")
public class GeocodingTransform extends Transform<StructuredRecord, StructuredRecord> {
  GeoApiContext geoApiContext;
  private final Conf config;
  private Schema outputSchema;

  // Build the schemas used in the story
  static private Schema latLngSchema = Schema.recordOf("LATLNG",
    Field.of("lat", Schema.of(Type.DOUBLE)),
    Field.of("lng", Schema.of(Type.DOUBLE))
  );

  static private Schema geometrySchema = Schema.recordOf("GEOMETRY",
    Field.of("latlng", latLngSchema)
  );

  static private Schema mapSchema = Schema.recordOf("MAP",
    Field.of("formattedAddress", Schema.of(Type.STRING)),
    Field.of("geometry", geometrySchema)
  );

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {

    // nullable means this property is optional
    @Name("apiKey")
    @Description("The Google Maps API key used to perform requests.")
    private String apiKey;

    @Name("addressFieldName")
    @Description("The name of the input field that contains an address to be processed.")
    private String addressFieldName;

    @Name("geocodingFieldName")
    @Description("The name of the output field that will contain the geocoding.")
    private String geocodingFieldName;

    @Name("outputSchema")
    private String outputSchema;

    public Schema getOutputSchema() throws IOException {
      return Schema.parseJson(outputSchema);
    }

    public String getAddressFieldName() {
      return addressFieldName;
    }

    public String getGeocodingFieldName() {
      return geocodingFieldName;
    }

    public String getApiKey() {
      return apiKey;
    }

  } // Conf


  public GeocodingTransform(Conf config) {
    System.out.println("GeocodingTransform::GeolocationTransform");
    this.config = config;
  } // GeocodingTransform


  // configurePipeline is called only once, when the pipeline is deployed. Static
  // validation should be done here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    System.out.println("GeocodingTransform::configurePipeline");
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    // the output schema is always the same as the input schema1


    if (config.getApiKey() == null) {
      throw new ValidationException(
        Arrays.asList(
          new ValidationFailure("No Google Maps API key supplied").withConfigProperty("apiKey")
        )
      );
    }

    if (config.getAddressFieldName() == null) {
      throw new ValidationException(
        Arrays.asList(
          new ValidationFailure("No input address field name supplied").withConfigProperty("addressFieldName")
        )
      );
    }

    if (config.getGeocodingFieldName() == null) {
      throw new ValidationException(
        Arrays.asList(
          new ValidationFailure("No output geocoding field name supplied").withConfigProperty("geocodingFieldName")
        )
      );
    }
    Schema inputSchema = stageConfigurer.getInputSchema();

    if (inputSchema.getField(config.getAddressFieldName()) == null) {
      throw new ValidationException(
        Arrays.asList(
          new ValidationFailure(
            String.format("No field called %s found in input schema", config.getAddressFieldName())
          ).withConfigProperty("addressFieldName")
        )
      );
    }

    try {
      ArrayList<Field> newFields = new ArrayList<>(inputSchema.getFields());
      newFields.add(Field.of(config.getGeocodingFieldName(), mapSchema));

      //stageConfigurer.setOutputSchema(outputSchema);
      stageConfigurer.setOutputSchema(Schema.recordOf("XXX", newFields));
    }
    catch(Exception e) {
      e.printStackTrace();
    }
  } // configurePipeline


  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    System.out.println("GeocodingTransform::initialize");
    geoApiContext = new GeoApiContext.Builder().apiKey(config.getApiKey()).build();
    outputSchema = context.getOutputSchema();
  } // initialize

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord inputRecord, Emitter<StructuredRecord> emitter) throws Exception {
    System.out.println("GeocodingTransform::transform");
    //System.out.println("Output schema: " + outputSchema.toString());
    String addressFieldName = config.getAddressFieldName();
    String address = inputRecord.get(addressFieldName);

    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema);
    CDAPUtils.copy(inputRecord, outputSchema, outputBuilder);
    GeocodingResult results[] = GeocodingApi.geocode(geoApiContext, address).await();
    outputBuilder.set(config.geocodingFieldName, 
      StructuredRecord.builder(mapSchema)
        .set("formattedAddress", results[0].formattedAddress)
        .set("geometry", 
          StructuredRecord.builder(geometrySchema)
            .set("latlng",
              StructuredRecord.builder(latLngSchema)
                .set("lat", results[0].geometry.location.lat)
                .set("lng", results[0].geometry.location.lng)
              .build())
          .build())
      .build()
    );
    //outputBuilder.set("latlng", StructuredRecord.builder(latLngSchema).set("lat", 1.0).set("lng", 1.0).build());

    /*
    String jsonResultString;
    try {
      GeocodingResult results[] = GeocodingApi.geocode(geoApiContext, address).await();
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      System.out.println("result: " + gson.toJson(results[0]));
      jsonResultString = gson.toJson(results[0]);
    } catch(Exception e) {
      e.printStackTrace();
      jsonResultString = "";
    }
    outputBuilder.set(config.getGeocodingFieldName(), jsonResultString);
    */
    emitter.emit(outputBuilder.build());
    // emitter.emit(record);
  } // transform


  @Override
  public void prepareRun(StageSubmitterContext context) {
    System.out.println("GeocodingTransform::prepareRun");
    // geoApiContext = new GeoApiContext.Builder().apiKey(config.apiKey).build();
  } // prepareRun


  @Override
  public void destroy() {
    System.out.println("GeocodingTransform::destroy");
    geoApiContext.shutdown();
  } // destroy

}