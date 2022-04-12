package blog.couchbase.solace;

import static com.couchbase.client.java.kv.LookupInSpec.get;
import static com.couchbase.client.java.kv.MutateInSpec.upsert;

import java.time.Duration;
import java.util.Arrays;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.MutateInResult;

public class UpdateHotelData {

	private static UpdateHotelData hotelData = null;

	private Bucket bucket;
	private Collection collection;

	//Singleton pattern. We create only a single Couchbase connection per bucket
	public static UpdateHotelData getInstance() {
		if (hotelData == null) {
			hotelData = new UpdateHotelData();
			hotelData.connectToCouchbase();
		}
		return hotelData;
	}

	private void connectToCouchbase()  {
		// Create the initial connection to the Couchbase Capella Cluster

		// Update this to your cluster
		String endpoint = "cb.8c-f5di7pqrfjy8.cloud.couchbase.com";
		String username = "dbuser";
		String password = "yourpassword";
		// User Input ends here.

		String bucketName = "travel-sample";

		// Cluster Environment configuration
		ClusterEnvironment env = ClusterEnvironment.builder()
				.securityConfig(
						SecurityConfig.enableTls(true).trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
				.ioConfig(IoConfig.enableDnsSrv(true)).build();

		// Initialize the Connection and provide database user credentials
		Cluster cluster = Cluster.connect(endpoint, ClusterOptions.clusterOptions(username, password).environment(env));

		// Create bucket object
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(Duration.parse("PT10S"));
		// create collection object for the 'hotel' collection located in the 'inventory' scope
		collection = bucket.scope("inventory").collection("hotel");
	}


	public void upsertHotelData(String content) {
		System.out.println("Updating Couchbase ...");
		JsonObject hotelData = null;

		try {
			hotelData = JsonObject.fromJson(content);

			//Instead of resolving the entire document we use the sub-document API
			//to only resolve the relevant parts of the hotel document prior to the update.
			//Here we can define the path to the relevant attributes of the document.
			//In our scenario 'pets_ok' and 'free_internet' are at the document root.
			LookupInResult result = collection.lookupIn(hotelData.getString("hotel_id"),
					Arrays.asList(get("pets_ok"), get("free_internet")));

			//Display the current values prior to the document update
			System.out.println("Values prior to update: 'pets_ok': " +  result.contentAs(0, String.class) + ", 'free_internet': " + result.contentAs(1, String.class));

			System.out.println("UPSERTING hotel config to Couchbase .....");

			//Even here we use the sub-document API.
			//Instead of updating the entire document we only update the relevant attributes.
			//The UPSERT method will either update the attributes if they already exist or create them in case they do not exist in within the document
			MutateInResult upsertResult = collection.mutateIn(hotelData.getString("hotel_id"),
					Arrays.asList(upsert("pets_ok", hotelData.getBoolean("pets_ok")),
							upsert("free_internet", hotelData.getBoolean("free_internet"))));

			//Resolve and display the updated attributes
			LookupInResult result1 = collection.lookupIn(hotelData.getString("hotel_id"),
					Arrays.asList(get("pets_ok"), get("free_internet")));
			System.out.println("Values after the update: 'pets_ok': " +  result1.contentAs(0, String.class) + ", 'free_internet': " + result1.contentAs(1, String.class));
		} catch (PathNotFoundException e) {
			System.out.println("Sub-doc path not found!");
		} catch (InvalidArgumentException e) {
			System.out.println("Cannot convert String to JsonObject!");
		}
	}
}
