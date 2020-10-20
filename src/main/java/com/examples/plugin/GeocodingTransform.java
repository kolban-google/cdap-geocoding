
package com.examples.plugin;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

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

  }

  public GeocodingTransform(Conf config) {
    System.out.println("GeolocationTransform::GeolocationTransform");
    this.config = config;
  }

  // configurePipeline is called only once, when the pipeline is deployed. Static
  // validation should be done here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    System.out.println("GeolocationTransform::configurePipeline");
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    // the output schema is always the same as the input schema1
    Schema inputSchema = stageConfigurer.getInputSchema();

    try {
      //stageConfigurer.setOutputSchema(outputSchema);
      stageConfigurer.setOutputSchema(null);
    }
    catch(Exception e) {
      e.printStackTrace();
    }

  }

  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    System.out.println("GeolocationTransform::initialize");
    geoApiContext = new GeoApiContext.Builder().apiKey(config.apiKey).build();
    outputSchema = config.getOutputSchema();
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord inputRecord, Emitter<StructuredRecord> emitter) throws Exception {
    System.out.println("GeolocationTransform::transform");
    System.out.println("Output schema: " + outputSchema.toString());
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema);
    CDAPUtils.copy(inputRecord, outputSchema, outputBuilder);
    GeocodingResult results[] = GeocodingApi.geocode(geoApiContext, inputRecord.get(config.addressFieldName)).await();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println("result: " + gson.toJson(results[0]));
    String jsonResultString = gson.toJson(results[0]);
    outputBuilder.set(config.geocodingFieldName, jsonResultString);
    emitter.emit(outputBuilder.build());
    // emitter.emit(record);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) {
    System.out.println("GeolocationTransform::prepareRun");
    // geoApiContext = new GeoApiContext.Builder().apiKey(config.apiKey).build();
  }

  @Override
  public void destroy() {
    System.out.println("GeolocationTransform::destroy");
    geoApiContext.shutdown();
  }

 
}
