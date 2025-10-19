// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0
//
//  ThriftBinary.swift
//  TwitterApacheThrift
//
//  Created on 9/23/21.
//  Copyright © 2021 Twitter. All rights reserved.
//

import CoreFoundation
import Foundation

/// A class for reading values from thrift data
class ThriftCompactBinary {
    
    /// The buffer for holding the thrift data
    let readingBuffer: MemoryBuffer
  
    var offset: Int {
      return readingBuffer.offset
    }

    /// Initialize BinaryProtocol class with data
    /// - Parameter data: The thrift data for reading
    init(data: Data) {
        readingBuffer = MemoryBuffer(buffer: data)
    }

    /// Moves the read cursor back by one byte for the field type
    func moveReadCursorBackAfterType() {
        readingBuffer.moveOffset(by: -(UInt8.bitWidth / 8))
    }

    /// Moves the read cursor back by two bytes for reading the field type and id
    func moveReadCursorBackAfterTypeAndFieldID() {
        moveReadCursorBackAfterType()
        readingBuffer.moveOffset(by: -(Int16.bitWidth / 8))
    }

//    func readThrift(type: ThriftType) throws -> ThriftObject {
//        return try readValue(index: nil, type: type)
//    }
//
//    func readStruct(index: Int?) throws -> ThriftStruct {
//        var fields: [Int: ThriftValue] = [:]
//        var nextField = try readFieldMetadata()
//        while nextField.type != .stop, let id = nextField.id {
//            let value = try readValue(index: id, type: nextField.type)
//            fields[id] = ThriftValue(index: id, type: nextField.type, data: value)
//            nextField = try readFieldMetadata()
//        }
//        return ThriftStruct(index: index, fields: fields)
//    }
//
//    func readValue(index: Int?, type: ThriftType, isCollection: Bool = false) throws -> ThriftObject {
//        switch type {
//        case .bool, .byte:
//            return .data(try Data([readByte()]))
//        case .double, .int64:
//            return .data(try readingBuffer.read(size: 8))
//        case .int16:
//            return .data(try readingBuffer.read(size: 2))
//        case .int32:
//            return .data(try readingBuffer.read(size: 4))
//        case .string:
//            return .data(try readBinary())
//        case .structure:
//            return .struct(try readStruct(index: index))
//        case .map:
//            var values: [ThriftKeyedCollection.Value] = []
//            let metadata = try readMapMetadata()
//            for _ in 0..<metadata.size {
//                let key = try readValue(index: nil, type: metadata.keyType)
//                let value = try readValue(index: nil, type: metadata.valueType)
//                values.append(.init(key: key, value: value))
//            }
//            return .keyedCollection(ThriftKeyedCollection(index: index,
//                                                          count: metadata.size,
//                                                          keyType:metadata.keyType,
//                                                          elementType: metadata.valueType,
//                                                          value: values))
//        case .list, .set:
//            var values: [ThriftObject] = []
//            let metadata = try readListMetadata()
//            for _ in 0..<metadata.size {
//                let value = try readValue(index: nil, type: metadata.elementType)
//                values.append(value)
//            }
//            return .unkeyedCollection(ThriftUnkeyedCollection(index: index, count: metadata.size, elementType: type, value: values))
//        default:
//            return .stop
//        }
//
//    }
//
//    /// Reads the next UInt64 from the thrift
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The value decoded from the thrift
//    func readUInt64() throws -> UInt64 {
//        let i64rd = try readingBuffer.read(size: 8)
//
//        let byte56 = UInt64(i64rd[0]) << 56
//        let byte48 = UInt64(i64rd[1]) << 48
//        let byte40 = UInt64(i64rd[2]) << 40
//        let byte32 = UInt64(i64rd[3]) << 32
//        let byte24 = UInt64(i64rd[4]) << 24
//        let byte16 = UInt64(i64rd[5]) << 16
//        let byte8 = UInt64(i64rd[6]) << 8
//        let byte0 = UInt64(i64rd[7]) << 0
//
//        return (byte56 | byte48 | byte40 | byte32 | byte24 | byte16 | byte8 | byte0)
//    }
//
//    /// Reads the next Int64 from the thrift
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The value decoded from the thrift
//    func readInt64() throws -> Int64 {
//        let u64 = try readUInt64()
//        return Int64(bitPattern: u64)
//    }
//
//    /// Reads the next Int32 from the thrift
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The value decoded from the thrift
//    func readInt32() throws -> Int32 {
//        let i32rd = try readingBuffer.read(size: 4)
//
//        let byte24 = Int32(i32rd[0]) << 24
//        let byte16 = Int32(i32rd[1]) << 16
//        let byte8 = Int32(i32rd[2]) << 8
//        let byte0 = Int32(i32rd[3]) << 0
//
//        return (byte24 | byte16 | byte8 | byte0)
//    }
//
    /// Reads the next Int16 from the thrift
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func superreadInt16() throws -> Int16 {
        let i16rd = try readingBuffer.read(size: 2)

        let byte8 = Int16(i16rd[0]) << 8
        let byte0 = Int16(i16rd[1]) << 0

        return byte8 | byte0
    }
//
//    /// Reads the next double from the thrift
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The value decoded from the thrift
//    func readDouble() throws -> Double {
//        let ui64 = try readUInt64()
//        return Double(bitPattern: ui64)
//    }
//
//    /// Reads the next data from the thrift. The length of data is based on the next 4 bytes.
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The value decoded from the thrift
//    func readBinary() throws -> Data {
//        let size = try readInt32()
//        return try readingBuffer.read(size: Int(size))
//    }
//
//    /// Reads the next UTF8 string from the thrift. The length of string data is based on the next 4 bytes.
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Throws: ThriftDecoderError.nonUTF8StringData when the string data is not a UTF8 string
//    /// - Returns: The value decoded from the thrift
//    func readString() throws -> String {
//        let stringData = try readBinary()
//
//        guard let string = String(bytes: stringData, encoding: .utf8) else {
//            throw ThriftDecoderError.nonUTF8StringData(stringData)
//        }
//
//        return string
//    }
//
    /// Reads the next byte from the thrift.
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: Returns true when the next byte is 1, otherwise returns false
    func readBool() throws -> Bool {
        return try readByte() == 1
    }

    /// Reads the next byte from the thrift.
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readByte() throws -> UInt8 {
        return try readingBuffer.read(size: 1)[0]
    }
//
//    /// Reads a map object metadata from the thrift. This type is equivalent to a Dictionary
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The type of key as a thrift type. The type of values as a thrift type. The amount of elements.
//    func readMapMetadata() throws -> (keyType: ThriftType, valueType: ThriftType, size: Int) {
//        let keyType = try readByte()
//        let valueType = try readByte()
//        let size = try readInt32()
//        return (try ThriftType(coreValue: keyType), try ThriftType(coreValue: valueType), Int(size))
//    }
//
//    /// Reads a set object metadata from the thrift.
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The type of values as a thrift type. The amount of elements.
//    func readSetMetadata() throws -> (elementType: ThriftType, size: Int) {
//        let type = try readByte()
//        let size = try readInt32()
//        return (try ThriftType(coreValue: type), Int(size))
//    }
//
//    /// Reads a list object metadata from the thrift. This type is equivalent to a Array
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The type of values as a thrift type. The amount of elements.
//    func readListMetadata() throws -> (elementType: ThriftType, size: Int) {
//        let type = try readByte()
//        let size = try readInt32()
//        return (try ThriftType(coreValue: type), Int(size))
//    }
//
//    /// Reads a the field metadata from the thrift. This type is equivalent to a Dictionary
//    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
//    /// - Returns: The field thrift type. The thrift id if it is not a ThriftType.stop.
//    func readFieldMetadata() throws -> (type: ThriftType, id: Int?) {
//        let type = try ThriftType(coreValue: try readByte())
//        if (type != .stop) {
//            let id = try self.readInt16()
//            return (type, Int(id))
//        }
//        return (type, nil)
//    }

    func readStruct(index: Int?) throws -> ThriftStruct {
        var fields: [Int: ThriftValue] = [:]
        var nextField = try readFieldMetadata(previousId: 0)
        while nextField.type != .stop, let id = nextField.id {
            let value = try readValue(index: id, type: nextField.type, isCollection: false)
            fields[id] = ThriftValue(index: id, type: nextField.type, data: value)
            nextField = try readFieldMetadata(previousId: id)
        }
        return ThriftStruct(index: index, fields: fields)
    }

    func readValue(index: Int?, type: ThriftType, isCollection: Bool = false) throws -> ThriftObject {
        switch type {
        case .void: //Void is boolean true in compact thrift when used as a field.
            return isCollection ? .stop : .data(Data([1]))
        case .bool: //bool is boolean false in compact thrift when used as a field but is one byte when used in a collection
            return isCollection ? .data(try Data([readByte()])) : .data(Data([0]))
        case .byte:
            return .data(try Data([readByte()]))
        case .double:
            return .data(try readingBuffer.read(size: 8))
        case .int64, .int16, .int32:
            return .data(try Data(unsignedLEBBytes()))
        case .string:
            return .data(try readBinary())
        case .structure:
            return .struct(try readStruct(index: index))
        case .map:
            var values: [ThriftKeyedCollection.Value] = []
            let metadata = try readMapMetadata()
            for _ in 0..<metadata.size {
                let key = try readValue(index: nil, type: metadata.keyType, isCollection: true)
                let value = try readValue(index: nil, type: metadata.valueType, isCollection: true)
                values.append(.init(key: key, value: value))
            }
            return .keyedCollection(ThriftKeyedCollection(index: index,
                                                          count: metadata.size,
                                                          keyType:metadata.keyType,
                                                          elementType: metadata.valueType,
                                                          value: values))
        case .list, .set:
            var values: [ThriftObject] = []
            let metadata = try readListMetadata()
            for _ in 0..<metadata.size {
                let value = try readValue(index: nil, type: metadata.elementType, isCollection: true)
                values.append(value)
            }
            return .unkeyedCollection(ThriftUnkeyedCollection(index: index, count: metadata.size, elementType: type, value: values))
        default:
            return .stop
        }

    }

    /// Reads a the field metadata from the thrift. This type is equivalent to a Dictionary
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The field thrift type. The thrift id if it is not a ThriftType.stop.
    func readFieldMetadata(previousId: Int) throws -> (type: ThriftType, id: Int?) {
        let binary = try readByte()
        if binary == 0 {
            return (.stop, nil)
        }
        let fieldIdDelta = UInt8((binary & 0xF0) >> 4)
        let fieldType = UInt8(binary & 0x0F)
        let type = try ThriftType(compactValue: fieldType)

        if fieldIdDelta == 0 {
            let fieldId = try Int16(zigZag: superreadInt16())
            return (type, Int(fieldId))
        }

        return (type, Int(fieldIdDelta) + previousId)
    }

    func readDouble() throws -> Double {
        let buffer = try readingBuffer.read(size: 8)
        let i64: UInt64 = buffer.withUnsafeBytes { $0.load(as: UInt64.self) }
        let value = CFSwapInt64LittleToHost(i64)
        return Double(bitPattern: value)
    }

    /// Reads the next UInt64 from the thrift
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readUInt64() throws -> UInt64 {
        let value: Int64 = decodeUnsignedLEB(from: readingBuffer.buffer)
        return CFSwapInt64LittleToHost(UInt64(zigZag: value))
    }

    /// Reads the next Int64 from the thrift
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readInt64() throws -> Int64 {
        let value: UInt64 = decodeUnsignedLEB(from: readingBuffer.buffer)
        return (Int64(zigZag: value))
    }

    /// Reads the next Int32 from the thrift
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readInt32() throws -> Int32 {
        let value: Int32 = decodeUnsignedLEB(from: readingBuffer.buffer)
        return (Int32(zigZag: value))
    }

    /// Reads the next Int16 from the thrift
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readInt16() throws -> Int16 {
        let value = try readInt32()
        return Int16(value)
    }

    /// Reads the next data from the thrift. The length of data is based on the next 4 bytes.
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The value decoded from the thrift
    func readBinary() throws -> Data {
        let bytes = try unsignedLEBBytes()
        let size: Int32 = decodeUnsignedLEB(from: bytes)
        return try readingBuffer.read(size: Int(size))
    }

    /// Reads a map object metadata from the thrift. This type is equivalent to a Dictionary
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The type of key as a thrift type. The type of values as a thrift type. The amount of elements.
    func readMapMetadata() throws -> (keyType: ThriftType, valueType: ThriftType, size: Int) {
        let binary = try readByte()
        if binary == 0 {
            //Empty map
            return (.stop, .stop, 0)
        }

        let sizeBytes = try unsignedLEBBytes(startingByte: binary)
        let size: Int32 = decodeUnsignedLEB(from: sizeBytes)

        let types = try readByte()
        let keyType = UInt8(types >> 4) & 0x0F
        let elementType = UInt8(types & 0x0F)

        return (try ThriftType(compactValue: keyType), try ThriftType(compactValue: elementType), Int(size))
    }

    /// Reads a list object metadata from the thrift. This type is equivalent to a Array
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The type of values as a thrift type. The amount of elements.
    func readListMetadata() throws -> (elementType: ThriftType, size: Int) {
        let binary = try readByte()
        let compactSize = UInt8(binary >> 4) & 0x0F
        let elementType = UInt8(binary & 0x0F)
        let type = try ThriftType(compactValue: elementType)

        //If size is 15 (1111) then it uses a different format
        if compactSize == 0b1111 {
            let sizeBytes = try unsignedLEBBytes()
            let size: Int32 = decodeUnsignedLEB(from: sizeBytes)
            return (type, Int(size))
        }

        return (type, Int(compactSize))
    }

    func unsignedLEBBytes(startingByte: UInt8? = nil) throws -> Data {
        var bytes: [UInt8] = []
        while true {
            let byte: UInt8
            if let firstByte = startingByte, bytes.isEmpty {
                byte = firstByte
            } else {
                byte = try readByte()
            }

            bytes.append(byte)
            if (byte & 0x80) == 0 {
                break
            }
        }
        return Data(bytes)
    }

    func decodeUnsignedLEB<T: BinaryInteger>(from bytes: Data) -> T {
        var result: T = 0
        var shift: T = 0

        for byte in bytes {
            result |= ((T(byte) & 0x7F) << shift)
            shift += 7
        }
        return result
    }
}
