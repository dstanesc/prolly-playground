import * as codec from '@ipld/dag-json'
import * as raw from 'multiformats/codecs/raw'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import * as Block from 'multiformats/block'
import bent from 'bent'
import { create, load } from 'prolly-trees/map'
import { bf, simpleCompare as compare } from 'prolly-trees/utils'
import { nocache } from 'prolly-trees/cache'

const getJSON = bent('json')

const chunker = bf(1000)

const cache = nocache

const opts = { cache, chunker, codec, hasher }

const storage = () => {
  const blocks = {}
  const put = block => {
    blocks[block.cid.toString()] = block
  }
  const get = async cid => {
    const block = blocks[cid.toString()]
    if (!block) throw new Error('Not found')
    return block
  }
  return { get, put, blocks }
}

const { get, put, blocks } = storage()

const getTestData = async () => {
  const data = await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/large-file.json')
  const list = data.map(elem => ({ key: elem.id, value: elem }))
  return list;
}

const createTree = async (list) => {
  let root
  for await (const node of create({ get, compare, list, ...opts })) {
    const address = await node.address
    await put(await node.block)
    root = node
  }
  return root;
}

const loadTree = async (cid) => {
  const root = await load({ cid, get, compare, ...opts })
  return root;
}

const loadValue = async (root, key) => {
  const value = await root.get(key)
  const result = value.result
  return result;
}

const performElementUpdate = (bulk, index, propName, propValue) => {
  console.log(`\nPerforming element update index=${index}, ${propName}=${propValue}`)
  const last = list[index]
  const key = last.key
  const value = last.value
  value[propName] = propValue
  bulk.push({ key, value })
}

const performUpdate = async (base) => {
  console.log("\nPerforming updates")
  const bulk = []
  performElementUpdate(bulk, list.length - 1, 'randomProperty1', 123)
  performElementUpdate(bulk, list.length - 2, 'randomProperty2', 'xyz')
  performElementUpdate(bulk, list.length - 3, 'randomProperty3', [1,2,3,4,5,6,7,8,9])
  const { blocks, root } = await base.bulk(bulk)
  await Promise.all(blocks.map(block => put(block)))
  console.log("\nNew Blocks")
  console.log("=============")
  let sum = 0
  blocks.forEach((block) => {
    sum += block.bytes.length
    console.log(`${block.cid.toString()} ${block.bytes.length} bytes`);
  })
  console.log(`Delta size ${(sum / (1024 * 1024)).toFixed(2)} MB`);
  return root
};

const debugBlocks = (blocks, prefix) => {

  console.log("\nAll blocks")
  console.log("=============")
  const cids = Object.keys(blocks);
  let sum = 0
  cids.forEach((cid, index) => {
    sum += blocks[cid].bytes.length
    console.log(`${cid.toString()} ${blocks[cid].bytes.length} bytes`);
  })
  console.log(`${prefix} size ${(sum / (1024 * 1024)).toFixed(2)} MB`);
}

const debugStoredEntries = async (root) => {
  const { result: entries } = await root.getAllEntries()
  for (const { key, value } of entries) {
    console.log(key);
    console.log(value);
  }
}

const list = await getTestData()

const root1 = await createTree(list)

debugBlocks(blocks, 'Before Update');

const root2 = await performUpdate(root1);

debugBlocks(blocks, 'After Update');


console.log(`\nRoot 1 cid ${await root1.address}`)
console.log(`\nRoot 2 cid ${await root2.address}`)

// debugStoredEntries(root);



