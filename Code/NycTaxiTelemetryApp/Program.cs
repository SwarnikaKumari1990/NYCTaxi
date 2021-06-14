using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Configuration;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Threading;
using System.Text;
using System.Globalization;

namespace NycTaxiTelemetryApp
{
    class Program
    {
        private static string startFileName = "NycTaxiStreamRideStart.json";
        private static string endFileName = "NycTaxiStreamRideEnd.json";

        private enum EventType {Pickup, Drop};

        private static string namespaceConnectionString;
        private static string startEventHubName;
        private static string endEventHubName;

        private static EventType eventType;

        private static EventHubClient eventHubClient;

        static void Main(string[] args)
        {
            #region Check type of events to send

            while(true)
            {
                Console.WriteLine();
                Console.WriteLine("*** Press 'P' for ride start (pickup) events or 'D' for ride end (drop) events ***");
                Console.WriteLine();
            
                var keyInfo = Console.ReadKey();

                if (keyInfo.Key.ToString().ToLower() == "p")
                {
                    eventType = EventType.Pickup;
                    break;
                }
                else if (keyInfo.Key.ToString().ToLower() == "d")
                {
                    eventType = EventType.Drop;
                    break;
                }
                else
                {
                    Console.WriteLine();
                    Console.WriteLine("Invalid input.");
                }
            }

            Console.WriteLine();
            Console.WriteLine("*** Preparing...");

            #endregion

            #region Read configuration and setup Azure Event Hubs client

            try
            {
                Setup();
            }
            catch(Exception)
            {
                return;
            }

            #endregion

            #region Send Events

            try
            {
                Console.WriteLine();
                Console.WriteLine("*** Use Escape key to pause and resume events anytime! ***");
                Console.WriteLine();
                Console.WriteLine($"*** All set! Press enter to start sending '{eventType}' events to Azure Event Hubs... ***");

                Console.ReadLine();

                SendEvents();
            }
            catch (Exception)
            {
                Console.WriteLine("Error in processing events.");
                Console.WriteLine("Press enter to exit...");
                Console.WriteLine();
            }

            #endregion            
        }

        private static void Setup()
        {
            // Read Configuration
            try
            {
                ReadConfiguration();
            }
            catch (Exception)
            {
                Console.WriteLine("Unable to read the configuration. Please ensure to add Event Hub Namespace connection string and Event Hub names in the app.settings file.");
                Console.WriteLine("Press enter to exit...");
                Console.WriteLine();

                throw;
            }

            // Create Event Hub client
            try
            {
                CreateEventHubClient();
            }
            catch (Exception)
            {
                Console.WriteLine("Unable to create Event Hub client. Ensure that configuration values are correct.");
                Console.WriteLine("Press enter to exit...");
                Console.WriteLine();

                throw;
            }
        }

        private static void ReadConfiguration()
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            namespaceConnectionString = config["EventHubNamespaceConnectionString"];
            startEventHubName = config["StartEventHubName"];
            endEventHubName = config["EndEventHubName"];

            if (String.IsNullOrEmpty(namespaceConnectionString) 
                || String.IsNullOrEmpty(startEventHubName)
                || String.IsNullOrEmpty(endEventHubName))
            {
                throw new Exception();
            }
        }

        private static void CreateEventHubClient()
        {
            string eventHubName;
            if (eventType == EventType.Pickup)
            {
                eventHubName = startEventHubName;
            }
            else
            {
                eventHubName = endEventHubName;
            }

            var eventHubConnectionString = namespaceConnectionString + ";EntityPath=" + eventHubName;

            eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionString);
        }

        private static void SendEvents()
        {
             var taxiEvents = LoadEventsFromFile();

            if (taxiEvents == null || taxiEvents.Count == 0)
            {
                return;
            }

            CultureInfo culture = new CultureInfo("en-IN");
            DateTime lastEventTime = DateTime.MinValue;
            bool isFirstEvent = true;
            int seconds = 0;
            int counter = 0;

            foreach (dynamic taxiEvent in taxiEvents)
            {
                // Pause and resume on Escape key
                if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Escape)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("*** Pausing the events. Press Esc key to continue ***");

                    while (!(Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Escape))
                    {
                        // do nothing
                    }

                    Console.WriteLine("*** Resuming the events! ***");
                    Console.WriteLine();
                }

                try
                {
                    DateTime newTime;
                    if (eventType == EventType.Pickup)
                    {
                        newTime = Convert.ToDateTime(taxiEvent.PickupTime.ToString(), culture);
                    }
                    else
                    {
                        newTime = Convert.ToDateTime(taxiEvent.DropTime.ToString(), culture);
                    }

                    if (isFirstEvent)
                    {
                        lastEventTime = newTime;
                        isFirstEvent = false;

                        Console.Write($"{eventType} Time: {lastEventTime} events - ");
                    }
                    else
                    {
                        seconds = Convert.ToInt32((newTime - lastEventTime).TotalSeconds);
                        lastEventTime = newTime;
                    }

                    if (seconds > 0)
                    {
                        Thread.Sleep(seconds * 1000);

                        Console.WriteLine();
                        Console.Write($"{eventType} Time: {lastEventTime} events - ");

                        counter = 0;
                    }

                    counter += 1;

                    Console.Write(counter + " ");

                    SendEvent(taxiEvent);
                }
                catch (Exception exception)
                {
                    Console.WriteLine("Error sending events.");
                    Console.WriteLine($"Error: {exception.Message}");                    
                    break;
                }
            }
        }

        private static dynamic LoadEventsFromFile()
        {
            string fileName;
            if (eventType == EventType.Pickup)
            {
                fileName = startFileName;
            }
            else
            {
                fileName = endFileName;
            }

            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "Data", fileName);
            string fileData = File.ReadAllText(filePath);
            
            return JsonConvert.DeserializeObject(fileData);
        }

        private static void SendEvent(dynamic taxiEvent)
        {
            string serializedEvent = JsonConvert.SerializeObject(taxiEvent);
            var eventData = new EventData(Encoding.UTF8.GetBytes(serializedEvent));

            eventHubClient.SendAsync(eventData);
        }
    }
}
