#/bin/sh

echo "Inject new Aleph2 bucket builder templates"

#############################

# Setup env

PROPERTY_CONFIG_FILE='/opt/infinite-install/config/infinite.configuration.properties'

cur_date=$(date +%Y-%m-%dT%TZ)

ADMIN_EMAIL=`grep "^admin.email=" $PROPERTY_CONFIG_FILE | sed s/'admin.email='// | sed s/' '//g`
if [ "$ADMIN_EMAIL" == "" ]; then
	ADMIN_EMAIL=infinite_default@ikanow.com
fi

#############################

# Install into DB

# NOTE: BE SURE TO MAKE A NEW ObjectId's (_id) WHEN YOU INSERT A NEW OBJECT. 
# THIS IS MONGODB'S PRIMARY KEY FOR EACH ENTRY.	

mongo <<EOF

/////////////////////////////////////////////////////////////
//
// Generic bucket templates

var json = cat('/opt/aleph2-home/webapps/templates/aleph2_bucket_builder/generic_bucket_templates.json');
var id = ObjectId("52f43a111111111000000010");

use social;
share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "aleph2-bucket-template",
"share": json, 
"title" : "Generic Aleph2 bucket builder templates", 
"description" : "Contains forms for building core Aleph2 functions into V2 buckets (via the V1 source editor)", 
"mediaType" : "application/java-archive", 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var x = db.share.findOne({_id: id}, {_id:1});
if (!x) db.share.insert(share);

/////////////////////////////////////////////////////////////
//
// Batch analytics templates

var json = cat('/opt/aleph2-home/webapps/templates/aleph2_bucket_builder/batch_analytics_templates.json');
var id = ObjectId("52f43a111111111000000020");

use social;
share={ 
"_id" : id, 
"created" : ISODate("$cur_date"), 
"modified" : ISODate("$cur_date"), 
"owner" : { "_id" : ObjectId("4e3706c48d26852237078005"), 
"email" : "$ADMIN_EMAIL", "displayName" : "Admin Infinite" }, 
"endorsed" : [  ObjectId("4c927585d591d31d7b37097a") ], 
"type" : "aleph2-bucket-template",
"share": json, 
"title" : "Batch analytics Aleph2 bucket builder templates", 
"description" : "Contains forms for building batch analytics and enrichment utilities", 
"mediaType" : "application/java-archive", 
"communities" : [ { 
		"_id" : ObjectId("4c927585d591d31d7b37097a"), 
		"name" : "Infinit.e System Community", 	
		"comment" : "Added by addWidgetsToMongo.sh" 
	} ] 
}

var x = db.share.findOne({_id: id}, {_id:1});
if (!x) db.share.insert(share);

EOF
