import { parsePoolKey } from "./events/core";
import { describe, expect, it } from "vitest";
import { parseByteArray, parseUint8Array } from "./parse";

describe("parse", () => {
  describe(parsePoolKey, () => {
    it("works correctly for random data from 0", () => {
      const result = parsePoolKey(
        ["0x5", "0x4", "0x3", "0x2", "0x1", "0x0"],
        0,
      );

      expect(result).toEqual({
        next: 5,
        value: {
          token0: 5n,
          token1: 4n,
          fee: 0x3n,
          tick_spacing: 0x2n,
          extension: 1n,
        },
      });
    });

    it("works correctly for random data from random place", () => {
      const result = parsePoolKey(
        ["0x5", "0x4", "0x3", "0x2", "0x1", "0x2", "0x3", "0x4"],
        3,
      );

      expect(result).toEqual({
        next: 8,
        value: {
          token0: 2n,
          token1: 1n,
          fee: 0x2n,
          tick_spacing: 0x3n,
          extension: 4n,
        },
      });
    });
  });

  describe(parseUint8Array, () => {
    it.each([
      {
        args: {
          data: [0x68656c6c6f20776f726c64n],
          startingFrom: 0,
        },
        expected: {
          value: "hello world",
          next: 1,
        },
      },
      {
        args: {
          data: [0x0],
          startingFrom: 0,
        },
        expected: {
          value: "",
          next: 1,
        },
      },
      {
        args: {
          data: [0x0, 0x68656c6c6f20776f726c64n],
          startingFrom: 1,
        },
        expected: {
          value: "hello world",
          next: 2,
        },
      },
    ])(
      `parseUint8Array($args.data, $args.startingFrom) = $expected.value`,
      ({ args: { data, startingFrom }, expected }) => {
        expect(
          parseUint8Array(
            data.map((x) => `0x${x.toString(16)}`) as readonly `0x${string}`[],
            startingFrom,
          ),
        ).toEqual({
          value: new TextEncoder().encode(expected.value),
          next: expected.next,
        });
      },
    );
  });

  describe(parseByteArray, () => {
    it.each([
      {
        data: [0n, 0x68656c6c6fn, 5n],
        startingFrom: 0,
        expected: { next: 3, value: "hello" },
      },
      {
        data: [123n, 0n, 0x68656c6c6fn, 5n],
        startingFrom: 1,
        expected: { next: 4, value: "hello" },
      },
      {
        data: [
          1n,
          0x4c6f6e6720737472696e672c206d6f7265207468616e203331206368617261n,
          0x63746572732en,
          6n,
        ],
        startingFrom: 0,
        expected: { next: 4, value: "Long string, more than 31 characters." },
      },
      {
        data: [
          0n,
          1n,
          0x4c6f6e6720737472696e672c206d6f7265207468616e203331206368617261n,
          0x63746572732en,
          6n,
          0n,
        ],
        startingFrom: 1,
        expected: { next: 5, value: "Long string, more than 31 characters." },
      },
      {
        data: [
          0x25a6c62b25db639c2a0fb26678f1ac2870e5fe8b22d3bd3eec09691d6755d8fn,
          0x2n,
          0x2320746573742070726f706f73616c206465736372697074696f6e0a0a6865n,
          0x6c6c6f20776f726c640a0a6060600a636f64650a6060600a0a232320746573n,
          0x740a0a6e6f74207265616c6c79n,
          0x0dn,
        ],
        startingFrom: 1,
        expected: {
          next: 6,
          value: `# test proposal description

hello world

\`\`\`
code
\`\`\`

## test

not really`,
        },
      },
    ])(
      "parseByteArray($data) = $expected",
      ({ data, startingFrom, expected }) => {
        expect(
          parseByteArray(
            data.map((x) => `0x${x.toString(16)}`) as readonly `0x${string}`[],
            startingFrom,
          ),
        ).toEqual(expected);
      },
    );
  });
});
