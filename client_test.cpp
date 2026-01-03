#include "mqtt_client.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <print>
#include <ranges>
#include <string>

using namespace ourmqtt;

void run_test_scenario() {

  // assumes you have a broker running on port 1883

  // Test 1: Client multipli
  std::println();
  std::println("=== TEST 1: Multiple Clients ===");

  std::vector<std::unique_ptr<MqttClient>> clients;

  // Crea 3 subscriber
  for (int i = 0; i < 3; i++) {
    auto client = std::make_unique<MqttClient>("subscriber_" + std::to_string(i));

    client->set_message_handler(
      [](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
        std::string message(std::from_range, payload);
        std::println(
            "on_message QOS_0 with topic: {}, QoS: {}, retain: {}, message: {}", 
            topic, qos, retain, message
          );
      });

    if (client->connect("localhost", 1883)) {
      client->subscribe("test/data", QOS_1);

      client->subscribe("test/qos0", QOS_0);
      client->subscribe("test/qos1", QOS_1);
      client->subscribe("test/qos2", QOS_2);
      clients.push_back(std::move(client));
    }
  }

  // Publisher
  MqttClient publisher("publisher_main");
  if (publisher.connect("localhost", 1883)) {
    for (int i = 0; i < 5; i++) {
      std::string msg = "Message #" + std::to_string(i);
      publisher.publish("test/data", msg, QOS_1);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  // Test 2: QoS Levels
  std::println();
  std::println("=== TEST 2: QoS Levels ===");

  publisher.publish("test/qos0", "QoS 0 message", QOS_0);
  publisher.publish("test/qos1", "QoS 1 message", QOS_1);
  publisher.publish("test/qos2", "QoS 2 message", QOS_2);

  // Test 3: Retained Messages
  std::println();
  std::println("=== TEST 3: Retained Messages ===");

  publisher.publish("test/retained", "This is retained", QOS_1, true);

  // Nuovo subscriber dovrebbe ricevere il messaggio retained
  std::this_thread::sleep_for(std::chrono::seconds(1));
  MqttClient late_subscriber("late_subscriber");
  late_subscriber.set_message_handler(
    [](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
      std::string message(std::from_range, payload);
      std::println(
          "[LATESUB] on_message QOS_0 with topic: {}, QoS: {}, retain: {}, message: {}", 
          topic, qos, retain, message
        );
    });

  if (late_subscriber.connect("localhost", 1883)) {
      late_subscriber.subscribe("test/retained", QOS_0);
  }

  // Test 4: Wildcards
  std::println();
  std::println("=== TEST 4: Wildcard Subscriptions ===");

  MqttClient wildcard_client("wildcard_sub");
  wildcard_client.set_message_handler(
    [](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
      std::string message(std::from_range, payload);
      std::println(
          "[WILDCARD] on_message QOS_0 with topic: {}, QoS: {}, retain: {}, message: {}", 
          topic, qos, retain, message
        );
    });

  if (wildcard_client.connect("localhost", 1883)) {
      wildcard_client.subscribe("sensors/+/temperature", QOS_0);
      wildcard_client.subscribe("logs/#", QOS_0);

      // Pubblica su vari topic
      publisher.publish("sensors/room1/temperature", "22°C", QOS_0);
      publisher.publish("sensors/room2/temperature", "24°C", QOS_0);
      publisher.publish("sensors/room1/humidity", "60%", QOS_0); // Non dovrebbe essere ricevuto
      publisher.publish("logs/error/app", "Error occurred", QOS_0);
      publisher.publish("logs/info/system", "System started", QOS_0);
  }

  std::println();
  std::println("Press Enter to finish tests...");
  std::cin.get();
}

int main() {
  std::println("=================================");
  std::println("    OurMQTT Complete Test Suite");
  std::println("=================================");
  std::println();

  try {
      run_test_scenario();
  }
  catch (const std::exception& e) {
      std::println(std::cerr, "Test failed: {}", e.what());
      return 1;
  }

  return 0;
}
