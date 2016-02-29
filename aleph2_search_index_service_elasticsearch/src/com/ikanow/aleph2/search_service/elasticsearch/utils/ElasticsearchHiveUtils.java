/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.client.Client;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchHiveOverrideBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;

import fj.data.Validation;

/** Utilities for managing the connection to the Hive metastore
 * @author Alex
 */
public class ElasticsearchHiveUtils {
	final static protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	public final static String MAIN_TABLE_NAME = "main_table";
	
	// Error strings
	public final static String ERROR_INVALID_MAIN_TABLE = "Main table missing from bucket {0}";
	public final static String ERROR_AUTO_SCHEMA_NOT_YET_SUPPORTED = "Currently the schema must be specified manually (bucket {0} table {1})";
	public final static String ERROR_SQL_QUERY_NOT_YET_SUPPORTED = "Currently SQL queries not specified as part of the main table (bucket {0})";
	public final static String ERROR_INVALID_MAIN_TABLE_FIELD = "Can't specify field {1} in main table of bucket {0}";
	public final static String ERROR_NO_VIEWS_ALLOWED_YET = "Currently views cannot be specified (bucket {0})";
	public final static String ERROR_SCHEMA_ERROR = "In bucket {0} table {1}, schema error = {2}";
	
	public final static String ERROR_HIVE_NOT_CONFIGURED = "Hive not installed on this system - contact your system administrator to add hive-site to Aleph2 yarn-config directory";
	
	private static final Set<String> _allowed_types = 
			ImmutableSet.<String>of(
					"TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "STRING", "BINARY", "TIMESTAMP", "DECIMAL",
					"DATE", "VARCHAR", "CHAR"
					);
	
	/** Basic validation of the data warehouse schema
	 * @param schema
	 * @param bucket
	 * @param security_service
	 * @return
	 */
	public static List<String> validateSchema(final DataSchemaBean.DataWarehouseSchemaBean schema, 
			final DataBucketBean bucket, 
			Optional<Client> maybe_client, ElasticsearchIndexServiceConfigBean config,
			final ISecurityService security_service)
	{
		final LinkedList<String> mutable_errs = new LinkedList<>();
		
		if (Optional.ofNullable(schema.enabled()).orElse(true)) {
			
			if (null == schema.main_table()) {
				mutable_errs.add(ErrorUtils.get(ERROR_INVALID_MAIN_TABLE, bucket.full_name()));
			}
			else {				
				// Currently: auto schema not supported
				if (Optional.ofNullable(schema.main_table().table_format()).orElse(Collections.emptyMap()).isEmpty()) {
					mutable_errs.add(ErrorUtils.get(ERROR_AUTO_SCHEMA_NOT_YET_SUPPORTED, bucket.full_name(), MAIN_TABLE_NAME));					
				}
				else { // check the schema is well formed
					final Validation<String, String> schema_str = generateFullHiveSchema(Optional.empty(), bucket, schema, maybe_client, config);
					schema_str.validation(fail -> mutable_errs.add(ErrorUtils.get(ERROR_SCHEMA_ERROR, bucket.full_name(), MAIN_TABLE_NAME, fail)), success -> true);
				}
				
				// Currently: sql query not supported
				if (Optional.ofNullable(schema.main_table().sql_query()).isPresent()) {
					mutable_errs.add(ErrorUtils.get(ERROR_SQL_QUERY_NOT_YET_SUPPORTED, bucket.full_name()));										
				}
				
				// Can't specify view name for main table
				if (Optional.ofNullable(schema.main_table().view_name()).isPresent()) {
					mutable_errs.add(ErrorUtils.get(ERROR_INVALID_MAIN_TABLE_FIELD, bucket.full_name(), "view_name"));										
				}								
			}
			// Currently: no views allowed
			
			if (!Optionals.ofNullable(schema.views()).isEmpty()) {
				mutable_errs.add(ErrorUtils.get(ERROR_NO_VIEWS_ALLOWED_YET, bucket.full_name()));				
			}
			
			//TODO (ALEPH-17): need permission to specify table name (wait for security service API to calm down)
		}		
		return mutable_errs;
	}

	public final static String getTableName(final DataBucketBean bucket, final DataSchemaBean.DataWarehouseSchemaBean schema) {
		return Optional.ofNullable(schema.main_table().database_name()).map(s -> s + ".").orElse("") + 					
				Optional.ofNullable(schema.main_table().name_override())
						.orElseGet(() -> BucketUtils.getUniqueSignature(bucket.full_name(), Optional.empty()));
	}
	
	/** Generates the command to drop a table to enable it to be updated
	 * @param bucket
	 * @param schema
	 * @return
	 */
	public final static String deleteHiveSchema(final DataBucketBean bucket, final DataSchemaBean.DataWarehouseSchemaBean schema) {
		return ErrorUtils.get("DROP TABLE IF EXISTS {0}", getTableName(bucket, schema)
				);
	}
	
	/** Handles the prefix and suffix of the full hive schema
	 *  https://www.elastic.co/guide/en/elasticsearch/hadoop/current/hive.html
	 * @param table_name - if empty then "main_table"
	 * @param bucket
	 * @param schema
	 * @param partial_hive_schema
	 * @return
	 */
	public static Validation<String, String> generateFullHiveSchema(
			final Optional<String> table_name,
			final DataBucketBean bucket, final DataSchemaBean.DataWarehouseSchemaBean schema, 
			Optional<Client> maybe_client, ElasticsearchIndexServiceConfigBean config
			)
	{		
		// (ignore views for the moment)

		final String prefix = ErrorUtils.get("CREATE EXTERNAL TABLE {0} ", getTableName(bucket, schema));
		
		final DataSchemaBean.DataWarehouseSchemaBean.Table table = 
				table_name.flatMap(t -> Optionals.ofNullable(schema.views()).stream().filter(v -> t.equals(v.database_name())).findFirst())
				.orElse(schema.main_table())
				;
		
		final JsonNode user_schema = _mapper.convertValue(table.table_format(), JsonNode.class);
		
		final Validation<String, String> partial_table = generatePartialHiveSchema(prefix, user_schema, true);

		// (for the main table, just going to be the full alias - for views will need to be cleverer)
		final String index = Optionals.of(() -> bucket.data_schema().search_index_schema().technology_override_schema().get(SearchIndexSchemaDefaultBean.index_name_override_).toString())
										.orElseGet(() -> "r__" + BucketUtils.getUniqueSignature(bucket.full_name(), Optional.empty()));
		
		final Optional<ElasticsearchHiveOverrideBean> maybe_override = 
				Optionals.of(() -> schema.technology_override_schema()).map(m -> BeanTemplateUtils.from(m, ElasticsearchHiveOverrideBean.class).get()) ;
		
		// OK all this horrible code is intended to sort out the list of types to apply in the hive query
		final Optional<ElasticsearchHiveOverrideBean.TableOverride> table_override = maybe_override.map(cfg -> cfg.table_overrides().get(table_name.orElse(MAIN_TABLE_NAME)));
		final Optional<Set<String>> user_type_overrides =  table_override.map(t -> t.types()).filter(l -> !l.isEmpty()).map(l -> new TreeSet<String>(l));
		final Set<String> mutable_type_set = 
				user_type_overrides
					.orElseGet(() -> { 
						return new TreeSet<String>(maybe_client.map(client -> ElasticsearchIndexUtils.getTypesForIndex(client, index)).orElse(Collections.emptySet())); 
					})
					;
		
		final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(bucket, config, _mapper);
		final CollidePolicy collide_policy = Optionals.of(() -> schema_config.search_technology_override().collide_policy()).orElse(CollidePolicy.new_type);
		
		Optionals.of(() -> schema_config.search_technology_override().type_name_or_prefix()).map(Optional::of)
			.orElseGet(() -> Optional.of((collide_policy == CollidePolicy.new_type) 
					? ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext.DEFAULT_PREFIX 
					: ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME)
			)
			.ifPresent(type_or_prefix -> {
				if (!user_type_overrides.isPresent()) { // leave alone if manually specified
					if (collide_policy == CollidePolicy.new_type) { // add a few types
						//TODO (ALEPH-17): need to make this get auto populated as new types are added, see the ALEPH-17 comment in ElasticsearchIndexService
						if (mutable_type_set.size() < 10) { 
							IntStream.rangeClosed(1, 10).boxed().map(i -> type_or_prefix + i.toString()).forEach(type -> mutable_type_set.add(type));
						}
					}
					else { // OK in this case just make sure the default type is represented
						mutable_type_set.add(type_or_prefix);					
					}
				}
			})
			;
		
		final String suffix = 
				Optional.
				of(" STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' ")
				.map(s -> s + ErrorUtils.get("TBLPROPERTIES(''es.index.auto.create'' = ''false'', ''es.resource'' = ''{0}/{1}''", index, mutable_type_set.stream().collect(Collectors.joining(","))))
				.map(s -> table_override.map(t -> t.name_mappings())
										.filter(m -> !m.isEmpty())
										.map(m -> s + ", 'es.mapping.names' = '" + 
														m.entrySet().stream().map(kv -> kv.getKey() +":" + kv.getValue()).collect(Collectors.joining(",")) +
														"'"
										).orElse(s)
				) 
				.map(s -> table_override.flatMap(t -> 
													Optional.ofNullable(t.url_query()).map(ss -> "?" + ss)														
																.map(Optional::of)
																.orElseGet(() -> Optional.ofNullable(t.json_query()).map(jq -> _mapper.convertValue(jq, JsonNode.class).toString()))
												)
										.map(ss -> s + ", 'es.query' = '" + ss + "'")
										.orElse(s)
				)
				.map(s -> s + ") ")
				.get(); 
		
		return partial_table.map(s -> s + suffix);
	}
	
	/** Creates the partial hive schema from the data warehouse schema bean
	 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
	 * @param prefix_string
	 * @param structure
	 * @return
	 */
	public static Validation<String, String> generatePartialHiveSchema(final String prefix_string, final JsonNode structure, final boolean top_level) {
		
		return Patterns.match(structure).<Validation<String, String>>andReturn()
			.when(TextNode.class, t -> _allowed_types.contains(t.asText()), t -> { //TODO: handle decimal with precision
				return Validation.success(prefix_string + t.asText()); 
			})
			.when(ObjectNode.class, o -> { // struct, format
				
				final String start_prefix = prefix_string + (top_level ? "(" : "STRUCT<");
				return Optionals.streamOf(o.fields(), false)
							.<Validation<String, String>>reduce(
								Validation.success(start_prefix)
								, 
								(acc, kv) -> {
									return acc.<Validation<String, String>>validation(
											fail -> Validation.fail(fail),
											success -> {
												final String pre_prefix = Lambdas.get(() -> {
													if (success.length() == start_prefix.length()) return "";
													else return ",";
												});
												return generatePartialHiveSchema(success + pre_prefix + kv.getKey() + (top_level ? " " : ": "), kv.getValue(), false);
											}
											)
											;
								}
								,
								(acc1, acc2) -> acc1 // (never called)
								)
								.map(success -> success + (top_level ? ")" : ">"))
							;
			})
			.when(ArrayNode.class, a -> 1 == a.size(), a -> { // array, format [ data_type ]
				return generatePartialHiveSchema(prefix_string + "ARRAY<", a.get(0), false).map(success -> success + ">");
			})
			.when(ArrayNode.class, a -> (a.size() > 1) && a.get(0).isObject(), a -> { // union, format [ {} data_type_1 ... ]
				final String start_prefix = prefix_string + "UNIONTYPE<";
				return Optionals.streamOf(a.iterator(), false).skip(1)
							.<Validation<String, String>>reduce(
									Validation.success(start_prefix)
									,
									(acc, j) -> {
										return acc.<Validation<String, String>>validation(
												fail -> Validation.fail(fail),
												success -> {
													final String pre_prefix = Lambdas.get(() -> {
														if (success.length() == start_prefix.length()) return "";
														else return ",";
													});
													return generatePartialHiveSchema(success + pre_prefix + " ", j, false);													
												});										
									}
									,
									(acc1, acc2) -> acc1 // (never called)
									)
									.map(success -> success + ">")
									;
			})
			.when(ArrayNode.class, a -> (2 == a.size()) && a.get(0).isTextual(), a -> { // map, format [ key value ]
				return generatePartialHiveSchema(prefix_string + "MAP<", a.get(0), false)
							.bind(success -> generatePartialHiveSchema(success + ", ", a.get(1), false))
							.map(success -> success + ">");
			})
			.otherwise(() -> Validation.fail(ErrorUtils.get("Unrecognized element in schema declaration after {0}: {1}", prefix_string, structure)))
			;		
	}
	
	//////////////////////////////////////////////////////////////////
	
	// SQL stuff
	
	/** Registers the hive table
	 *  NOT SURE HOW TO TEST THIS?
	 * @param config
	 * @param delete_prev
	 * @param create_new
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	public static Validation<String, Boolean> registerHiveTable(final Optional<Connection> maybe_hive_cxn, final Configuration config, Optional<String> delete_prev, Optional<String> create_new)  {
		
		final Tuple3<String, String, String> params = getParamsFromHiveConfig(config);
		final String connection_url = params._1();
		final String user_name = params._2();
		final String password = params._3();
						
		try {	
			Class.forName("org.apache.hive.jdbc.HiveDriver");

			final Connection hive_cxn = maybe_hive_cxn.orElseGet(Lambdas.wrap_u(() -> DriverManager.getConnection(connection_url, user_name, password)));
			
			final Validation<String, Boolean> delete_prev_results = 
					delete_prev.<Validation<String, Boolean>>map(sql -> {
						try {
							return Validation.success(hive_cxn.createStatement().execute(sql));
						}
						catch (Throwable t) {
							return Validation.fail(ErrorUtils.getLongForm("delete hive table, error = {0}", t));
						}
					})
					.orElse(Validation.success(true));
			
			final Validation<String, Boolean> create_new_results = delete_prev_results.bind(b -> {
				return create_new.<Validation<String, Boolean>>map(sql -> {
					try {
						return Validation.success(hive_cxn.createStatement().execute(sql));
					}
					catch (Throwable t) {
						return Validation.fail(ErrorUtils.getLongForm("create hive table, error = {0}", t));
					}
				})
				.orElse(Validation.success(b));			
			});
			
			return create_new_results;
		}
		catch (Throwable t) {
			return Validation.fail(ErrorUtils.getLongForm("connect to hive, error = {0}", t));
		}
	}	

	private static final Pattern hive_extractor = Pattern.compile("^.*[^/]+:[/][/]([^/]+)[/]([^/]+).*$");
	
	/** Pull out parameters from configuration
	 * @param config
	 * @return
	 */
	public static Tuple3<String, String, String> getParamsFromHiveConfig(final Configuration config) {
		
		final String username = config.get("javax.jdo.option.ConnectionUserName", "");
		final String password = "";
		final Matcher m = hive_extractor.matcher(config.get("javax.jdo.option.ConnectionURL", ""));
		
		final int port = config.getInt("hive.server2.thrift.port", 10000);
		
		final String connection = Lambdas.get(() -> {
			if (m.matches()) {
				//(table name is not needed when connecting this way)
				return  ErrorUtils.get("jdbc:hive2://{0}:{2,number,#}", m.group(1), m.group(2), port);
			}
			else return "";
		});		
		return Tuples._3T(connection, username, password);
	}
	
	/** 
	 * Retrieves the system configuration
	 *  (with code to handle possible internal concurrency bug in Configuration)
	 *  (tried putting a static synchronization around Configuration as an alternative)
	 * @return
	 */
	public static Configuration getHiveConfiguration(final GlobalPropertiesBean globals){		
		for (int i = 0; i < 60; ++i) {
			try { 
				return getHiveConfiguration(i, globals);
			}
			catch (java.util.ConcurrentModificationException e) {
				final long to_sleep = Patterns.match(i).<Long>andReturn()
						.when(ii -> ii < 15, __ -> 100L)
						.when(ii -> ii < 30, __ -> 250L)
						.when(ii -> ii < 45, __ -> 500L)
						.otherwise(__ -> 1000L)
						+ (new Date().getTime() % 100L) // (add random component)
						;
				
				try { Thread.sleep(to_sleep); } catch (Exception ee) {}
				if (59 == i) throw e;
			}
		}
		return null;
	}
	/** Support for strange concurrent modification exception
	 * @param try_number
	 * @return
	 */
	protected static Configuration getHiveConfiguration(int attempt, final GlobalPropertiesBean globals){
		synchronized (Configuration.class) {
			Configuration config = new Configuration(false);
			
			final String hive_config_file = globals.local_yarn_config_dir() +"/hive-site.xml";
			if (new File(hive_config_file).exists())
			{
				config.addResource(new Path(hive_config_file));
			}
			else {
				throw new RuntimeException(ERROR_HIVE_NOT_CONFIGURED);
			}
			if (attempt > 10) { // (try sleeping here)
				final long to_sleep = 500L + (new Date().getTime() % 100L); // (add random component)
				try { Thread.sleep(to_sleep); } catch (Exception e) {}
			}
			
			return config;
		}		
	}
	
}
