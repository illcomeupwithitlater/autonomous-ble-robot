#include <NimBLEDevice.h>

#define BEACON_NAME "XIAO-BEACON"
static const uint8_t MFG_DATA[] = { 'X','I','A','O','1','2','3','4' };

void setup() {
  Serial.begin(115200);

  NimBLEDevice::init(BEACON_NAME);
  NimBLEDevice::setDeviceName(BEACON_NAME);

  NimBLEAdvertisementData advData;
  advData.setName(BEACON_NAME);
  advData.setManufacturerData(std::string((char*)MFG_DATA, sizeof(MFG_DATA)));

  NimBLEAdvertisementData scanResp;
  scanResp.setName(BEACON_NAME);

  NimBLEAdvertising *pAdvertising = NimBLEDevice::getAdvertising();
  pAdvertising->setAdvertisementData(advData);
  pAdvertising->setScanResponseData(scanResp);

  pAdvertising->setMinInterval(0x00A0);
  pAdvertising->setMaxInterval(0x00A0);

  NimBLEDevice::setPower(ESP_PWR_LVL_P3);

  pAdvertising->start();
  Serial.println("Started advertising");
}

void loop() {
  delay(1000);
}
