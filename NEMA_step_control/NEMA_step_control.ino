const int stepPin = 2;
const int dirPin  = 3;

void setup() {
  pinMode(stepPin, OUTPUT);
  pinMode(dirPin, OUTPUT);

  digitalWrite(stepPin, LOW);
  digitalWrite(dirPin, LOW);

  Serial.begin(115200);
  while (!Serial);

  Serial.println("Enter steps (e.g. 200 or -200):");
}

void stepMotor(int steps) {
  bool dir = (steps >= 0);
  digitalWrite(dirPin, dir ? LOW : HIGH);
  delayMicroseconds(10);

  int numSteps = abs(steps);  

  for (int i = 0; i < numSteps; i++) {
    digitalWrite(stepPin, HIGH);
    delayMicroseconds(5);
    digitalWrite(stepPin, LOW);
    delayMicroseconds(2000); // adjust speed here
  }
}

void loop() {
  if (Serial.available()) {
    String input = Serial.readStringUntil('\n');
    input.trim();

    if (input.length() > 0) {
      int steps = input.toInt();

      Serial.print("Moving ");
      Serial.print(steps);
      Serial.println(" steps");

      stepMotor(steps);

      Serial.println("Done. Enter next value:");
    }
  }
}