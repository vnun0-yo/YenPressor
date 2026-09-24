YenPressor — a fast, modern archive extractor written in C. Supports ZIP, RAR, 7z, TAR, GZ, XZ, and more. Uses native binaries for maximum speed with libarchive fallback. Features live progress bar, password prompt, recursive extraction, and colored terminal output.

## Installation

```bash
git clone https://github.com/vnun0-yo/YenPressor.git
```

```bash
cd YenPressor
```

# install gcc

```bash
sudo apt install gcc 
```

# install devalopment tools

```bash
sudo apt install libarchive-dev libssl-dev zlib1g-dev
```

```bash
sudo apt install p7zip-full
```

```bash
sudo apt install p7zip-full unrar
```

```bash
sudo apt install p7zip-full unrar unzip
```

## run the tool 

```bash
gcc -O2 -std=c11 -Wall -Wextra -Wno-unused-parameter -pthread -o YenPressor YenPressor.c -larchive -lz -lcrypto
```

##examples

```bash
./YenPressor -h 
```

```bash
./YenPressor test.zip
```

# if the zip or rar file  have a password use option -p set the password

# example 

```bash
./YenPressor test.zip -p 'password'
```

## The Tool by - Yen 
