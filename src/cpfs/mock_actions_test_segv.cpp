/* Copyright 2013 ClusterTech Ltd */

int main() {
  *reinterpret_cast<char*>(0) = '1';
  return 0;
}
