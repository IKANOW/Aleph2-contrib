package controllers;

import com.ikanow.aleph2.access_manager.data_access.AccessDriver;
import com.ikanow.aleph2.data_model.interfaces.data_layers.IManagementDbService;

import play.*;
import play.mvc.*;
import views.html.*;

public class Application extends Controller {

    public static Result index() {
        return ok(index.render("Your new application is ready."));
    }
    
    public static Result test() {
    	//get the service the user needs to touch
		//TODO should we be passing the usergroup along with this so security manager can check permissions
		IManagementDbService management_db_service = AccessDriver.getAccessContext().getManagementDbService();
		//if user has permission to access this service, check if they can access the api calls we need to make
		//TODO
		
		//if they have access to the api call, go ahead and perform the operations
		
		//return a result (this is probably an api call so we should usually return something)
    			
    	return ok("test message");
    }

}
