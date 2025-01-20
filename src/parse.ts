export interface Parser<T> {
  (
    data: readonly `0x${string}`[],
    startingFrom: number,
  ): {
    value: T;
    next: number;
  };
}

export function parseSpanOf<T>(type: Parser<T>): Parser<T[]> {
  return (data, startingFrom) => {
    const numElements = Number(data[startingFrom]);

    const elements: T[] = [];
    let index = startingFrom + 1;

    while (elements.length < numElements) {
      const { value, next } = type(data, index);
      index = next;
      elements.push(value);
    }

    return {
      value: elements,
      next: index,
    };
  };
}

export const parseU128: Parser<bigint> = (data, startingFrom) => {
  return {
    value: BigInt(data[startingFrom]),
    next: startingFrom + 1,
  };
};

export const parseU64 = parseU128;

export const parseU256: Parser<bigint> = (data, startingFrom) => {
  return {
    value:
      BigInt(data[startingFrom]) + BigInt(data[startingFrom + 1]) * 2n ** 128n,
    next: startingFrom + 2,
  };
};

export const parseI129: Parser<bigint> = (data, startingFrom) => {
  return {
    value:
      BigInt(data[startingFrom]) *
      (BigInt(data[startingFrom + 1]) !== 0n ? -1n : 1n),
    next: startingFrom + 2,
  };
};

export type GetParserType<T extends Parser<any>> =
  T extends Parser<infer U> ? U : never;

export const parseU8: Parser<number> = (data, startingFrom) => {
  return {
    value: Number(data[startingFrom]),
    next: startingFrom + 1,
  };
};

export const parseFelt252: Parser<bigint> = (data, startingFrom) => {
  return {
    value: BigInt(data[startingFrom]),
    next: startingFrom + 1,
  };
};

export const parseAddress: Parser<bigint> = parseFelt252;

export const parseBoolean: Parser<boolean> = (data, startingFrom) => {
  let num = BigInt(data[startingFrom]);
  let value: boolean;
  if (num === 0n) {
    value = false;
  } else {
    if (num === 1n) {
      value = true;
    } else {
      throw new Error("Invalid boolean value");
    }
  }
  return {
    value,
    next: startingFrom + 1,
  };
};

/**
 * Returns a parser that will only run if there is additional data in the event to be parsed
 * @param parser the parser that it will run if there is additional data
 */
export function backwardsCompatibleParserAdditionalArgument<T>(
  parser: Parser<T>,
): Parser<T | null> {
  return (data, startingFrom) => {
    if (startingFrom < data.length) {
      return parser(data, startingFrom);
    }
    return { value: null, next: startingFrom };
  };
}

export function combineParsers<
  T extends {
    [key: string]: unknown;
  },
>(parsers: {
  [K in keyof T]: { index: number; parser: Parser<T[K]> };
}): Parser<T> {
  return (data, startingFrom) =>
    Object.entries(parsers)
      .sort(([, { index: index0 }], [, { index: index1 }]) => {
        return index0 - index1;
      })
      .reduce(
        (memo, fieldParser) => {
          const { value: parsedValue, next } = fieldParser[1].parser(
            data,
            memo.next,
          );
          memo.value[fieldParser[0] as keyof T] = parsedValue;
          memo.next = next;
          return memo;
        },
        {
          value: {} as Partial<T>,
          next: startingFrom,
        },
      ) as {
      value: T;
      next: number;
    };
}

export const parseUint8Array: Parser<Uint8Array> = (data, startingFrom) => {
  const { next, value } = parseFelt252(data, startingFrom);

  const result: number[] = [];
  for (let i = 0; i < 31; i++) {
    const position = BigInt(i * 8);
    const byte = (value & (255n << position)) >> position;
    if (byte === 0n) {
      break;
    }
    result.unshift(Number(byte));
  }

  return {
    value: new Uint8Array(result),
    next,
  };
};

const parseByteArrayWords = parseSpanOf(parseUint8Array);

export const parseByteArray: Parser<string> = (data, startingFrom) => {
  const words = parseByteArrayWords(data, startingFrom);

  const value = new TextDecoder().decode(
    new Uint8Array(
      Buffer.concat([
        ...words.value,
        // pending word
        parseUint8Array(data, words.next).value,
      ]),
    ),
  );

  return {
    // pending word length is not used
    next: words.next + 2,
    value,
  };
};
