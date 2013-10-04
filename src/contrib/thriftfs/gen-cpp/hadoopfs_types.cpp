/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#include "hadoopfs_types.h"



const char* ThriftHandle::ascii_fingerprint = "56A59CE7FFAF82BCA8A19FAACDE4FB75";
const uint8_t ThriftHandle::binary_fingerprint[16] = {0x56,0xA5,0x9C,0xE7,0xFF,0xAF,0x82,0xBC,0xA8,0xA1,0x9F,0xAA,0xCD,0xE4,0xFB,0x75};

uint32_t ThriftHandle::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->id);
          this->__isset.id = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t ThriftHandle::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("ThriftHandle");
  xfer += oprot->writeFieldBegin("id", ::apache::thrift::protocol::T_I64, 1);
  xfer += oprot->writeI64(this->id);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* Pathname::ascii_fingerprint = "EFB929595D312AC8F305D5A794CFEDA1";
const uint8_t Pathname::binary_fingerprint[16] = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

uint32_t Pathname::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->pathname);
          this->__isset.pathname = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t Pathname::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("Pathname");
  xfer += oprot->writeFieldBegin("pathname", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->pathname);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* FileStatus::ascii_fingerprint = "68FE7242A6727149B291A86FB9570D2B";
const uint8_t FileStatus::binary_fingerprint[16] = {0x68,0xFE,0x72,0x42,0xA6,0x72,0x71,0x49,0xB2,0x91,0xA8,0x6F,0xB9,0x57,0x0D,0x2B};

uint32_t FileStatus::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->path);
          this->__isset.path = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->length);
          this->__isset.length = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->isdir);
          this->__isset.isdir = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_I16) {
          xfer += iprot->readI16(this->block_replication);
          this->__isset.block_replication = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->blocksize);
          this->__isset.blocksize = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 6:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->modification_time);
          this->__isset.modification_time = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 7:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->permission);
          this->__isset.permission = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 8:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->owner);
          this->__isset.owner = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 9:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->group);
          this->__isset.group = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t FileStatus::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("FileStatus");
  xfer += oprot->writeFieldBegin("path", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->path);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("length", ::apache::thrift::protocol::T_I64, 2);
  xfer += oprot->writeI64(this->length);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("isdir", ::apache::thrift::protocol::T_BOOL, 3);
  xfer += oprot->writeBool(this->isdir);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("block_replication", ::apache::thrift::protocol::T_I16, 4);
  xfer += oprot->writeI16(this->block_replication);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("blocksize", ::apache::thrift::protocol::T_I64, 5);
  xfer += oprot->writeI64(this->blocksize);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("modification_time", ::apache::thrift::protocol::T_I64, 6);
  xfer += oprot->writeI64(this->modification_time);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("permission", ::apache::thrift::protocol::T_STRING, 7);
  xfer += oprot->writeString(this->permission);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("owner", ::apache::thrift::protocol::T_STRING, 8);
  xfer += oprot->writeString(this->owner);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("group", ::apache::thrift::protocol::T_STRING, 9);
  xfer += oprot->writeString(this->group);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* BlockLocation::ascii_fingerprint = "8BF3B16BED96367B21783389BDF8B1C0";
const uint8_t BlockLocation::binary_fingerprint[16] = {0x8B,0xF3,0xB1,0x6B,0xED,0x96,0x36,0x7B,0x21,0x78,0x33,0x89,0xBD,0xF8,0xB1,0xC0};

uint32_t BlockLocation::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->hosts.clear();
            uint32_t _size0;
            ::apache::thrift::protocol::TType _etype3;
            iprot->readListBegin(_etype3, _size0);
            this->hosts.resize(_size0);
            uint32_t _i4;
            for (_i4 = 0; _i4 < _size0; ++_i4)
            {
              xfer += iprot->readString(this->hosts[_i4]);
            }
            iprot->readListEnd();
          }
          this->__isset.hosts = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->names.clear();
            uint32_t _size5;
            ::apache::thrift::protocol::TType _etype8;
            iprot->readListBegin(_etype8, _size5);
            this->names.resize(_size5);
            uint32_t _i9;
            for (_i9 = 0; _i9 < _size5; ++_i9)
            {
              xfer += iprot->readString(this->names[_i9]);
            }
            iprot->readListEnd();
          }
          this->__isset.names = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->offset);
          this->__isset.offset = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->length);
          this->__isset.length = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t BlockLocation::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("BlockLocation");
  xfer += oprot->writeFieldBegin("hosts", ::apache::thrift::protocol::T_LIST, 1);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->hosts.size()));
    std::vector<std::string> ::const_iterator _iter10;
    for (_iter10 = this->hosts.begin(); _iter10 != this->hosts.end(); ++_iter10)
    {
      xfer += oprot->writeString((*_iter10));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("names", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->names.size()));
    std::vector<std::string> ::const_iterator _iter11;
    for (_iter11 = this->names.begin(); _iter11 != this->names.end(); ++_iter11)
    {
      xfer += oprot->writeString((*_iter11));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("offset", ::apache::thrift::protocol::T_I64, 3);
  xfer += oprot->writeI64(this->offset);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("length", ::apache::thrift::protocol::T_I64, 4);
  xfer += oprot->writeI64(this->length);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* TBlock::ascii_fingerprint = "EA2D65F1E0BB78760205682082304B41";
const uint8_t TBlock::binary_fingerprint[16] = {0xEA,0x2D,0x65,0xF1,0xE0,0xBB,0x78,0x76,0x02,0x05,0x68,0x20,0x82,0x30,0x4B,0x41};

uint32_t TBlock::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->blockId);
          this->__isset.blockId = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->numBytes);
          this->__isset.numBytes = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->generationStamp);
          this->__isset.generationStamp = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t TBlock::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TBlock");
  xfer += oprot->writeFieldBegin("blockId", ::apache::thrift::protocol::T_I64, 1);
  xfer += oprot->writeI64(this->blockId);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("numBytes", ::apache::thrift::protocol::T_I64, 2);
  xfer += oprot->writeI64(this->numBytes);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("generationStamp", ::apache::thrift::protocol::T_I64, 3);
  xfer += oprot->writeI64(this->generationStamp);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* TDatanodeID::ascii_fingerprint = "EFB929595D312AC8F305D5A794CFEDA1";
const uint8_t TDatanodeID::binary_fingerprint[16] = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

uint32_t TDatanodeID::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->name);
          this->__isset.name = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t TDatanodeID::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TDatanodeID");
  xfer += oprot->writeFieldBegin("name", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->name);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* TLocatedBlock::ascii_fingerprint = "0E6FCDC9A70E2B8DA64F3D5FD23D3D55";
const uint8_t TLocatedBlock::binary_fingerprint[16] = {0x0E,0x6F,0xCD,0xC9,0xA7,0x0E,0x2B,0x8D,0xA6,0x4F,0x3D,0x5F,0xD2,0x3D,0x3D,0x55};

uint32_t TLocatedBlock::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->block.read(iprot);
          this->__isset.block = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->namespaceId);
          this->__isset.namespaceId = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->dataTransferVersion);
          this->__isset.dataTransferVersion = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->location.clear();
            uint32_t _size12;
            ::apache::thrift::protocol::TType _etype15;
            iprot->readListBegin(_etype15, _size12);
            this->location.resize(_size12);
            uint32_t _i16;
            for (_i16 = 0; _i16 < _size12; ++_i16)
            {
              xfer += this->location[_i16].read(iprot);
            }
            iprot->readListEnd();
          }
          this->__isset.location = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t TLocatedBlock::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TLocatedBlock");
  xfer += oprot->writeFieldBegin("block", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->block.write(oprot);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("namespaceId", ::apache::thrift::protocol::T_I32, 2);
  xfer += oprot->writeI32(this->namespaceId);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("dataTransferVersion", ::apache::thrift::protocol::T_I32, 3);
  xfer += oprot->writeI32(this->dataTransferVersion);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldBegin("location", ::apache::thrift::protocol::T_LIST, 4);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(this->location.size()));
    std::vector<TDatanodeID> ::const_iterator _iter17;
    for (_iter17 = this->location.begin(); _iter17 != this->location.end(); ++_iter17)
    {
      xfer += (*_iter17).write(oprot);
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* MalformedInputException::ascii_fingerprint = "EFB929595D312AC8F305D5A794CFEDA1";
const uint8_t MalformedInputException::binary_fingerprint[16] = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

uint32_t MalformedInputException::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->message);
          this->__isset.message = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t MalformedInputException::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("MalformedInputException");
  xfer += oprot->writeFieldBegin("message", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->message);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

const char* ThriftIOException::ascii_fingerprint = "EFB929595D312AC8F305D5A794CFEDA1";
const uint8_t ThriftIOException::binary_fingerprint[16] = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

uint32_t ThriftIOException::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->message);
          this->__isset.message = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t ThriftIOException::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("ThriftIOException");
  xfer += oprot->writeFieldBegin("message", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->message);
  xfer += oprot->writeFieldEnd();
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


