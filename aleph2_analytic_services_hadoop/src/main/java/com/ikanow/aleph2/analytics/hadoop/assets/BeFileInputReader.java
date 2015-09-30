/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.analytics.hadoop.data_model.BeJobBean;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable;
import com.ikanow.aleph2.analytics.hadoop.data_model.IParser;
import com.ikanow.aleph2.analytics.hadoop.services.BeJobLauncher;
import com.ikanow.aleph2.analytics.hadoop.services.BeJsonParser;
import com.ikanow.aleph2.analytics.hadoop.services.BeStreamParser;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;

/** The input reader specific to batch enrichment modules
 * @author Alex
 */
public class BeFileInputReader extends  RecordReader<String, Tuple2<Long, IBatchRecord>> implements IBeJobConfigurable{

	/** Simple implementation of IBatchRecord
	 * @author Alex
	 */
	public static class BatchRecord implements IBatchRecord {
		public BatchRecord(final JsonNode json, final ByteArrayOutputStream content) {
			_json = json;
			_content = content;
		}		
		public JsonNode getJson() { return _json; }
		public Optional<ByteArrayOutputStream> getContent() { return Optional.ofNullable(_content); }		
		protected final JsonNode _json; protected final ByteArrayOutputStream _content;
	}
	
	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);
	public static String DEFAULT_GROUPING = "daily";

	protected CombineFileSplit _fileSplit;
	protected InputStream _inStream = null;
	protected FileSystem _fs;
	protected Configuration _config;
	protected int _currFile = 0;
	protected int _numFiles = 1;
	
	protected String _currrentFileName = null;

	private Tuple2<Long, IBatchRecord> _record;

	protected IEnrichmentModuleContext _enrichmentContext;

	protected DataBucketBean _dataBucket;

	protected SharedLibraryBean _beSharedLibrary;

	protected EnrichmentControlMetadataBean _ecMetadata;
	protected static Map<String, IParser> _parsers = new HashMap<String, IParser>();
	static{
		_parsers.put("JSON", new BeJsonParser());
		_parsers.put("BIN", new BeStreamParser());
	}
	
	Date start = null;
	@SuppressWarnings("unused")
	private int batchSize;
	@SuppressWarnings("unused")
	private IEnrichmentBatchModule _enrichmentBatchModule;
	
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
		this.start =  new Date();
		final String contextSignature = context.getConfiguration().get(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE);   
		try {
			this._enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
			this._dataBucket = _enrichmentContext.getBucket().get();
			this._beSharedLibrary = _enrichmentContext.getModuleConfig();		
			this._ecMetadata = BeJobBean.extractEnrichmentControlMetadata(_dataBucket, context.getConfiguration().get(BatchEnrichmentJob.BE_META_BEAN_PARAM));
		} catch (Exception e) {
			throw new IOException(e);
		}

		final String jobName = _config.get("mapred.job.name", "unknown");
		logger.info(jobName + ": new split, contains " + _numFiles + " files, total size: " + _fileSplit.getLength());		
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (_currFile >= _numFiles) {
			return false;
		}
		
		if (null == _inStream){
			
			// Step 1: get input stream
			_fs = FileSystem.get(_config);
			try {
				_inStream = _fs.open(_fileSplit.getPath(_currFile));
			}
			catch (FileNotFoundException e) { // probably: this is a spare mapper, and the original mapper has deleted this file using renameAfterParse
				_currFile++;
				if (_currFile < _numFiles) {
					_inStream.close();
					_inStream = null;
					return nextKeyValue();		// (just a lazy way of getting to the next file)		
				}
				else {
					return false; // all done
				}
			}
		}	 // instream = null		
		this._currrentFileName = _fileSplit.getPath(_currFile).toString();
		IParser parser = getParser(_currrentFileName);
		_record = parser.getNextRecord(_currFile,_currrentFileName,_inStream);
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
		if(!parser.multipleRecordsPerFile()){
			archiveOrDeleteFile();
			_currFile++;
			_inStream.close();
			_inStream = null;
		}
		return true;
	}

	/**
	 * 
	 */
	private void archiveOrDeleteFile() {
		try {
			if (_dataBucket.data_schema()!=null && _dataBucket.data_schema().storage_schema()!=null && _dataBucket.data_schema().storage_schema().enabled()) {
				Path currentPath = _fileSplit.getPath(_currFile);
				_fs.rename(currentPath, createArchivePath(currentPath));
			} else {
				_fs.delete(_fileSplit.getPath(_currFile), false);
			}
		} catch (Exception e) {
			logger.error(ErrorUtils.getLongForm(HadoopErrorUtils.EXCEPTION_CAUGHT, e));
			// We're just going to move on if we can't delete the file, it's
			// probably a permissions error
		}
	}

	private Path createArchivePath(Path currentPath) throws Exception {
		

		ChronoUnit timeGroupingUnit = ChronoUnit.DAYS;
		try {
			timeGroupingUnit = TimeUtils.getTimePeriod(Optionals.of(() -> _dataBucket.data_schema().storage_schema().processed().grouping_time_period()).orElse(DEFAULT_GROUPING)).success();			
		} catch (Throwable t) {			
			logger.error(ErrorUtils.getLongForm(HadoopErrorUtils.VALIDATION_ERROR,t),t);
		}
		String timeGroupingFormat = TimeUtils.getTimeBasedSuffix(timeGroupingUnit,Optional.of(ChronoUnit.MINUTES));
		SimpleDateFormat sdf = new SimpleDateFormat(timeGroupingFormat);
		String timeGroup = sdf.format(start);
		Path storedPath = Path.mergePaths(currentPath.getParent().getParent(),new Path("/stored/processed/"+timeGroup));

		return storedPath;
	}

	protected  IParser getParser(String fileName) {
		IParser parser = null;
		
		if(fileName!=null){
			 int dotPos =  fileName.lastIndexOf("."); 
			String ext = fileName.substring(dotPos+1).toUpperCase();  
			parser = _parsers.get(ext);
			// default to binary
			if(parser == null){
				parser = _parsers.get("BIN");
			}
		}
		
		return parser;
	}

	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		return _currrentFileName;
	}

	@Override
	public Tuple2<Long, IBatchRecord> getCurrentValue() throws IOException, InterruptedException {
		return _record;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)_currFile/(float)_numFiles;
	}

	@Override
	public void close() throws IOException {
		if (null != _inStream) {
			_inStream.close();
		}
		if (null != _fs) {
			_fs.close();
		}		
	}

	@Override
	public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata) {
		this._ecMetadata = ecMetadata;
	}

	@Override
	public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary) {
		this._beSharedLibrary = beSharedLibrary;
	}

	@Override
	public void setDataBucket(DataBucketBean dataBucketBean) {
		this._dataBucket = dataBucketBean;
		
	}
		
	
	@Override
	public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext) {
		this._enrichmentContext = enrichmentContext;
	}

	@Override
	public void setBatchSize(int size) {
		this.batchSize = size;
		
	}

	@Override
	public void setEnrichmentBatchModule(IEnrichmentBatchModule beModule) {
		this._enrichmentBatchModule = beModule;
		
	}
	
	

}
