using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Gremlin.Net;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;
using static Gremlin.Net.Process.Traversal.Order;
using static Gremlin.Net.Process.Traversal.Operator;
using static Gremlin.Net.Process.Traversal.Pop;
using static Gremlin.Net.Process.Traversal.Scope;
using static Gremlin.Net.Process.Traversal.TextP;
using static Gremlin.Net.Process.Traversal.Column;
using static Gremlin.Net.Process.Traversal.Direction;
using static Gremlin.Net.Process.Traversal.T;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;

namespace Aws4RequestSigner
{
    class Program
    {
        // Gremlin queries that will be executed.
        private static Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
        {
            { "Cleanup",        "g.V().drop()" },
            { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
            { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
            { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
            { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
            { "AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
            { "AddEdge 2",      "g.V('thomas').addE('knows').to(g.V('ben'))" },
            { "AddEdge 3",      "g.V('ben').addE('knows').to(g.V('robin'))" },
            { "UpdateVertex",   "g.V('thomas').property('age', 44)" },
            { "CountVertices",  "g.V().count()" },
            { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
            { "Project",        "g.V().hasLabel('person').values('firstName')" },
            { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
            { "Traverse",       "g.V('thomas').out('knows').hasLabel('person')" },
            { "Traverse 2x",    "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
            { "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
            { "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
            { "CountEdges",     "g.E().count()" },
            { "DropVertex",     "g.V('thomas').drop()" },
        };


        static void Main(string[] args)
        {

            var access_key = "youraccesskey";
            var secret_key = "yoursecretkey";
            var neptune_endpoint = "yourneptuneendpoint"; // ex: mycluster.cluster.us-east-1.neptune.amazonaws.com
            var neptune_region = "us-east-1"; //ex: us-east-1
            var using_ELB = false; //Set to True if using and ELB and define ELB URL via ELB_endpoint variable
            var ELB_endpoint = ""; //ex: myelb.elb.us-east-1.amazonaws.com

            /* The AWS4RequestSigner library was intented to pass a signed request to an HTTP endpoint.
             * Since we're using websockets, we will create the HTTP request and sign the request, however
             * we will pull the headers from the signed request in order to create a webSocketConfiguration
             * object with these same headers. 
             */
            var neptunesigner = new AWS4RequestSigner(access_key, secret_key);
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri("http://" + neptune_endpoint + "/gremlin")
            };
            var signedrequest = neptunesigner.Sign(request, "neptune-db", neptune_region);
            var authText = signedrequest.Headers.GetValues("Authorization").FirstOrDefault();
            var authDate = signedrequest.Headers.GetValues("x-amz-date").FirstOrDefault();

            var webSocketConfiguration = new Action<ClientWebSocketOptions>(options => {
                options.SetRequestHeader("host", neptune_endpoint);
                options.SetRequestHeader("x-amz-date", authDate);
                options.SetRequestHeader("Authorization", authText);
            });

            /* GremlinServer() accepts the hostname and port as separate parameters.  
             * Split the endpoint into both variables to pass to GremlinServer()
             *
             * Also - determine if an ELB is used.  If using an ELB, connect using the ELB hostname.
             */
            var neptune_host = "";
            if (using_ELB)
            {
                neptune_host = ELB_endpoint;
            }
            else
            {
                neptune_host = neptune_endpoint.Split(':')[0];
            }
            var neptune_port = int.Parse(neptune_endpoint.Split(':')[1]);

            var gremlinServer = new GremlinServer(neptune_host, neptune_port);

            
            var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration);
            var remoteConnection = new DriverRemoteConnection(gremlinClient);
            var g = Traversal().WithRemote(remoteConnection);

            /* Example code to pull the first 5 vertices in a graph. */
            Console.WriteLine("Get List of Node Labels:");
          


            foreach(var query in gremlinQueries)
            {
                Console.WriteLine(String.Format("Running this query: {0}: {1}", query.Key, query.Value));

                // Create async task to execute the Gremlin query.
                var resultSet = SubmitRequest(gremlinClient, query).Result;

                if (resultSet.Count > 0)
                {
                    Console.WriteLine("\tResult:");
                    foreach (var result in resultSet)
                    {
                        // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                        string output = JsonConvert.SerializeObject(result);
                        Console.WriteLine($"\t{output}");
                    }
                    Console.WriteLine();
                }

              
                PrintStatusAttributes(resultSet.StatusAttributes);
                Console.WriteLine();

            }
           
            
        }

        private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, KeyValuePair<string, string> query)
        {
            try
            {
                return gremlinClient.SubmitAsync<dynamic>(query.Value);
            }
            catch (ResponseException e)
            {
                Console.WriteLine("\tRequest Error!");

                // Print the Gremlin status code.
                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                PrintStatusAttributes(e.StatusAttributes);
               
                throw;
            }
        }

        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            
        }
        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }



    }

}
