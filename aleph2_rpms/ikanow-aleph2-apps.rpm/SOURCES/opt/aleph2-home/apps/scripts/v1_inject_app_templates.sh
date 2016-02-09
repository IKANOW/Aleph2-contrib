#/bin/sh

echo "Inject or update Aleph2 bucket builder templates"

#############################

# Setup env

PROPERTY_CONFIG_FILE='/opt/infinite-install/config/infinite.configuration.properties'

cur_date=$(date +%Y-%m-%dT%TZ)

ADMIN_EMAIL=`grep "^admin.email=" $PROPERTY_CONFIG_FILE | sed s/'admin.email='// | sed s/' '//g`
if [ "$ADMIN_EMAIL" == "" ]; then
	ADMIN_EMAIL=infinite_default@ikanow.com
fi

export ENRICH_FILE=$(ls /opt/aleph2-home/apps/plugins/aleph2_enrichment_utils*.jar)
export FLUME_FILE=$(ls /opt/aleph2-home/apps/plugins/aleph2_flume_harvester*.jar)
export LOGSTASH_FILE=$(ls /opt/aleph2-home/apps/plugins/aleph2_logstash_harvester*.jar)
export SCRIPT_FILE=$(ls /opt/aleph2-home/apps/plugins/aleph2_script_harvester*.jar)
export STORM_FILE=$(ls /opt/aleph2-home/apps/plugins/aleph2_storm_script_topo*.jar)

#############################

# Install into DB

# NOTE: BE SURE TO MAKE A NEW ObjectId's (_id) WHEN YOU INSERT A NEW OBJECT. 
# THIS IS MONGODB'S PRIMARY KEY FOR EACH ENTRY.	

mongo <<EOF

/////////////////////////////////////////////////////////////
//
// Enrichment Utils

var id = ObjectId("52f43a222222222000000010");

use social;
var share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "binary",
"title" : "/app/aleph2/library/enrichment_utils.jar", 
"description" : "com.ikanow.aleph2.DummyEntryPoint", 
"mediaType" : "application/java-archive", 
"documentLocation" : 
	{ "collection" : "$ENRICH_FILE" }, 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var curr = db.share.findOne( { "_id" : id } , { _id : 1 } );
if (curr) db.share.update( { "_id" : id } , { \$set: { title: share.title, modified: share.modified, documentLocation: { "collection" : share.documentLocation.collection } } }, false, false );
if (!curr) db.share.save(share);

/////////////////////////////////////////////////////////////
//
// Flume Harvester

var id = ObjectId("52f43a222222222000000020");

use social;
var share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "binary",
"title" : "/app/aleph2/library/flume_harvester.jar", 
"description" : "com.ikanow.aleph2.example.flume_harvester.services.FlumeHarvestTechnology", 
"mediaType" : "application/java-archive", 
"documentLocation" : 
	{ "collection" : "$FLUME_FILE" }, 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var curr = db.share.findOne( { "_id" : id } , { _id : 1 } );
if (curr) db.share.update( { "_id" : id } , { \$set: { title: share.title, modified: share.modified, documentLocation: { "collection" : share.documentLocation.collection } } }, false, false );
if (!curr) db.share.save(share);

/////////////////////////////////////////////////////////////
//
// Logstash Harvester

var id = ObjectId("52f43a222222222000000030");

use social;
var share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "binary",
"title" : "/app/aleph2/library/logstash_harvester.jar", 
"description" : "com.ikanow.aleph2.harvest.logstash.services.LogstashHarvestService", 
"mediaType" : "application/java-archive", 
"documentLocation" : 
	{ "collection" : "$LOGSTASH_FILE" }, 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var curr = db.share.findOne( { "_id" : id } , { _id : 1 } );
if (curr) db.share.update( { "_id" : id } , { \$set: { title: share.title, modified: share.modified, documentLocation: { "collection" : share.documentLocation.collection } } }, false, false );
if (!curr) db.share.save(share);

/////////////////////////////////////////////////////////////
//
// Script Harvester

var id = ObjectId("52f43a222222222000000040");

use social;
var share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "binary",
"title" : "/app/aleph2/library/script_harvester.jar", 
"description" : "com.ikanow.aleph2.harvest.script.services.ScriptHarvestService", 
"mediaType" : "application/java-archive", 
"documentLocation" : 
	{ "collection" : "$SCRIPT_FILE" }, 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var curr = db.share.findOne( { "_id" : id } , { _id : 1 } );
if (curr) db.share.update( { "_id" : id } , { \$set: { title: share.title, modified: share.modified, documentLocation: { "collection" : share.documentLocation.collection } } }, false, false );
if (!curr) db.share.save(share);

/////////////////////////////////////////////////////////////
//
// Storm Enrichment Engine

var id = ObjectId("52f43a222222222000000040");

use social;
var share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "binary",
"title" : "/app/aleph2/library/storm_js_topology.jar", 
"description" : "com.ikanow.aleph2.storm.samples.topology.JavaScriptTopology2", 
"mediaType" : "application/java-archive", 
"documentLocation" : 
	{ "collection" : "$STORM_FILE" }, 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var curr = db.share.findOne( { "_id" : id } , { _id : 1 } );
if (curr) db.share.update( { "_id" : id } , { \$set: { title: share.title, modified: share.modified, documentLocation: { "collection" : share.documentLocation.collection } } }, false, false );
if (!curr) db.share.save(share);

EOF
