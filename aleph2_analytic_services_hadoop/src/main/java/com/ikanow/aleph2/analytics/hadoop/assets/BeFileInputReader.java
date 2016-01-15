/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable;
import com.ikanow.aleph2.analytics.hadoop.data_model.IParser;
import com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext;
import com.ikanow.aleph2.analytics.hadoop.services.BeJobLauncher;
import com.ikanow.aleph2.analytics.hadoop.services.BeJsonParser;
import com.ikanow.aleph2.analytics.hadoop.services.BeStreamParser;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

/** The input reader specific to batch enrichment modules
 * @author Alex
 */
public class BeFileInputReader extends  RecordReader<String, Tuple2<Long, IBatchRecord>> implements IBeJobConfigurable{

	/** Simple implementation of IBatchRecord
	 *  TODO: move this into core shared
	 * @author jfreydank
	 */
	public static class BatchRecord implements IBatchRecord, Serializable {
		private static final long serialVersionUID = -1059865997758583525L;
		
		private static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		/** Serialize
		 * @param oos
		 * @throws IOException
		 */
		private void writeObject(ObjectOutputStream oos) throws IOException {
			oos.writeUTF(_json.toString());
			if (null != _content) {
				oos.write(_content.toByteArray());
			}			
		}
		/** Deserialize
		 * @param ois
		 * @throws JsonProcessingException
		 * @throws IOException
		 */
		private void readObject(ObjectInputStream ois) throws JsonProcessingException, IOException {
			_json = _mapper.readTree(ois.readUTF());
			int remaining = ois.available();
			if (remaining > 0) {
				_content = new ByteArrayOutputStream();
				byte[] b = new byte[remaining];
				ois.read(b);
				_content.write(b);
			}
		}
		
		/** USer c'tor
		 * @param json
		 * @param content
		 */
		public BatchRecord(final JsonNode json, final ByteArrayOutputStream content) {
			_json = json;
			_content = content;
		}		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getJson()
		 */
		public JsonNode getJson() { return _json; }
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getContent()
		 */
		public Optional<ByteArrayOutputStream> getContent() { return Optional.ofNullable(_content); }
		
		protected JsonNode _json; 
		protected ByteArrayOutputStream _content;
	}
	
	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected final String _my_uuid = UuidUtils.get().getRandomUuid();
	
	protected CombineFileSplit _fileSplit;
	protected InputStream _inStream = null;
	protected FileSystem _fs;
	protected Configuration _config;
	protected int _currFile = 0;
	protected int _numFiles = 1;
	protected int _numRecords = 0;
	protected int _maxRecords = Integer.MAX_VALUE;
	protected IParser _parser = null;
	
	protected String _currentFileName = null;

	private Tuple2<Long, IBatchRecord> _record;

	protected IEnrichmentModuleContext _enrichmentContext;

	protected DataBucketBean _dataBucket;

	protected static Map<String, IParser> _parsers = new HashMap<String, IParser>();
	static{
		_parsers.put("JSON", new BeJsonParser());
		_parsers.put("BIN", new BeStreamParser());
	}
	
	protected Date start = null;
	
	/** User c'tor
	 */
	public BeFileInputReader(){
		super();
		logger.debug("BeFileInputReader.constructor");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException{				
		_config = context.getConfiguration();
		_fileSplit = (CombineFileSplit) inputSplit;
		_numFiles = _fileSplit.getNumPaths();
		
		_maxRecords = _config.getInt(BatchEnrichmentJob.BE_DEBUG_MAX_SIZE, Integer.MAX_VALUE);
		
		this.start =  new Date();
		final String contextSignature = context.getConfiguration().get(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE);   
		try {
			this._enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
			this._dataBucket = _enrichmentContext.getBucket().get();
		} catch (Exception e) {
			/**/
			
			//TODO nicer to put the serialized data schema somewhere else?
			this._dataBucket = BeanTemplateUtils.build(DataBucketBean.class).done().get();
			
			//throw new IOException(e);
		}

		final String jobName = _config.get("mapred.job.name", "unknown");
		logger.info(jobName + ": new split, contains " + _numFiles + " files, total size: " + _fileSplit.getLength());		
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		_numRecords++;
		if (_numRecords > _maxRecords) {
			if (null != _inStream) {
				_inStream.close();
			}
			return false;
		}
		if (_currFile >= _numFiles) {
			return false;
		}
		
		if (null == _inStream){
			
			// Step 1: get input stream
			_fs = FileSystem.get(_config);
			try {
				// To ensure atomicity, first rename the file:
				// BUT ONLY IF IT'S MY FILE!
				final Path in = _fileSplit.getPath(_currFile);
				if (in.toString().contains(IStorageService.TO_IMPORT_DATA_SUFFIX)) {
					final Path renamed = in.suffix(_my_uuid);
					_fs.rename(in, renamed);
					_inStream = _fs.open(renamed);
				}
				else {
					_inStream = _fs.open(in);
				}
			}
			catch (FileNotFoundException e) { // probably: this is a spare mapper, and the original mapper has deleted this file using renameAfterParse
				_currFile++;
				if (_currFile < _numFiles) {
					if (null != _inStream) _inStream.close();
					_inStream = null;
					return nextKeyValue();		// (just a lazy way of getting to the next file)		
				}
				else {
					return false; // all done
				}
			}
			this._currentFileName = _fileSplit.getPath(_currFile).toString();
			_parser = getParser(_currentFileName);
		}	 // instream = null		
		
		_record = _parser.getNextRecord(_currFile,_currentFileName,_inStream);
		if (null == _record) { // Finished this file - are there any others?
			archiveOrDeleteFile();
			_currFile++;
			if (_currFile < _numFiles) {
				_inStream.close();
				_inStream = null;
				return nextKeyValue();				
			}
			else {
				return false; // all done
			}
		} // record = null
		// close stream if not multiple records per file supported
		if(!_parser.multipleRecordsPerFile()){
			archiveOrDeleteFile();
			_currFile++;
			_inStream.close();
			_inStream = null;
		}
		return true;
	}

	/** For input files (pure enrichment, not when used for analytics), deletes or archives the files following completion
	 */
	private void archiveOrDeleteFile() {
		try {
			final Path currentPath = _fileSplit.getPath(_currFile);
			// First check - if only want to do anything if this is an internal job:
			if (!currentPath.toString().contains(IStorageService.TO_IMPORT_DATA_SUFFIX)) {
				return; // (not your file to modify....)
			}
			
			final boolean storage_enabled  = 
					Optional.ofNullable(_dataBucket.data_schema()).map(ds -> ds.storage_schema())
							.map(ss -> Optional.ofNullable(ss.enabled()).orElse(true))
							.orElse(false)
							;
			
			final boolean archive_enabled = storage_enabled && 
					Optionals.of(() -> _dataBucket.data_schema().storage_schema().raw())
							.map(raw -> Optional.ofNullable(raw.enabled()).orElse(true))
							.orElse(false)
							;
			
			if (archive_enabled) {
				Path newPath = createArchivePath(currentPath);
				_fs.mkdirs(newPath);
				
				@SuppressWarnings("unused")
				final boolean success = _fs.rename(currentPath.suffix(_my_uuid), Path.mergePaths(newPath, new Path("/" + currentPath.getName())));				
				
			} else {
				_fs.delete(currentPath.suffix(_my_uuid), false);
			}
		} catch (Exception e) {
			logger.error(ErrorUtils.getLongForm(HadoopErrorUtils.EXCEPTION_CAUGHT, e));
			// We're just going to move on if we can't delete the file, it's
			// probably a permissions error
		}
	}

	/** Returns the temporal (or not) directory in which to place raw files
	 * @param currentPath
	 * @return
	 * @throws Exception
	 */
	private Path createArchivePath(Path currentPath) throws Exception {

		final String timeGroupingFormat =
				TimeUtils.getTimePeriod(Optionals.of(() -> _dataBucket.data_schema().storage_schema().processed().grouping_time_period()).orElse(""))
				.validation(fail -> "", success -> TimeUtils.getTimeBasedSuffix(success,Optional.of(ChronoUnit.MINUTES)));
		
		final String timeGroup = timeGroupingFormat.isEmpty()
				? IStorageService.NO_TIME_SUFFIX
				: (new SimpleDateFormat(timeGroupingFormat)).format(start);
		
		Path storedPath = Path.mergePaths(currentPath.getParent().getParent().getParent().getParent() // (ie up 3 to the root, ie managed_bucket==first subdir)
								,
								new Path(IStorageService.STORED_DATA_SUFFIX_RAW + timeGroup));

		return storedPath;
	}

	/** Returns the parser
	 * @param fileName
	 * @return
	 */
	protected  IParser getParser(String fileName) {
		IParser parser = null;
		
		if(fileName!=null){
			 int dotPos =  fileName.lastIndexOf("."); 
			 if (dotPos >= 0) {
				String ext = fileName.substring(dotPos+1).toUpperCase();  
				parser = _parsers.get(ext);
			 }
			// default to binary
			if(parser == null){
				parser = _parsers.get("BIN");
			}
		}
		
		return parser;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		return _currentFileName;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public Tuple2<Long, IBatchRecord> getCurrentValue() throws IOException, InterruptedException {
		return _record;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)_currFile/(float)_numFiles;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	@Override
	public void close() throws IOException {
		if (null != _inStream) {
			_inStream.close();
		}
		if (null != _fs) {
			_fs.close();
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setDataBucket(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public void setDataBucket(DataBucketBean dataBucketBean) {
		this._dataBucket = dataBucketBean;
		
	}
		
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
	 */
	@Override
	public void setBatchSize(int size) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentContext(com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext)
	 */
	@Override
	public void setEnrichmentContext(BatchEnrichmentContext enrichmentContext) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(java.util.List)
	 */
	@Override
	public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
	}

}
