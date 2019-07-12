/* eslint-disable no-unused-vars */
import dynamooseModule from 'dynamoose';
import defaultLogger, {NO_MAX_OPTION_WARNING} from './logger';
import findService from './find';
import jsonify from './jsonify';

const getIndexKeysFromSchema = schema => {
  if (schema && schema.indexes) {
    return Object.values(schema.indexes.global).map(index => index.name);
  }
  const indexKeys = Object.keys(schema).filter(key => schema[key] && schema[key].index && schema[key].index.global);
  return Array.isArray(indexKeys) ? indexKeys : [];
};

const getHashKeyFromSchema = schema => schema instanceof dynamooseModule.Schema && schema.hashKey ?
  schema.hashKey.name :
  Object.keys(schema).filter(key => schema[key] && schema[key].hashKey)[0];

const getRangeKeyFromSchema = schema => schema instanceof dynamooseModule.Schema ?
  schema.rangeKey && schema.rangeKey.name :
  Object.keys(schema).filter(key => schema[key] && schema[key].rangeKey)[0];

export const {Schema} = dynamooseModule;
export const DEFAULT_DYNAMOOSE_OPTIONS = {
  create: false,
  update: false,
  waitForActive: false,
  streamOptions: {
    enable: false
  },
  serverSideEncryption: false
};

export class Service {
  constructor(options, dynamooseOptions = DEFAULT_DYNAMOOSE_OPTIONS, dynamoose = dynamooseModule, logger = defaultLogger) {
    this.options = options || {};
    this.logger = logger;
    if (!this.options.paginate || !this.options.paginate.max) {
      this.logger.warn(NO_MAX_OPTION_WARNING);
    }
    this.paginate = this.options.paginate;
    if (this.options.localUrl) {
      dynamoose.local(this.options.localUrl);
    }
    const {modelName, schema} = this.options;
    this.hashKey = getHashKeyFromSchema(schema);
    this.rangeKey = getRangeKeyFromSchema(schema);
    this.indexKeys = getIndexKeysFromSchema(schema);
    this.model = dynamoose.model(modelName, schema, dynamooseOptions);
    this.id = this.hashKey;
    this.jsonify = jsonify(schema);
  }

  get keys() {
    const {hashKey, rangeKey, indexKeys} = this;
    return {hashKey, rangeKey, indexKeys}
  }

  async find(params = {query: {}}) {
    return findService(this.options.schema)(this.model, this.keys).find(params);
  }

  async get(id, params = {}) {
    params.paginate = false
    params.query = { ...(params.query || {}), [this.hashKey]: id };
    const result = await findService(this.options.schema)(this.model, this.keys).find(params);
    return result && result.length && result.shift()
  }

  async create(data, params) {
    if (Array.isArray(data)) {
      return Promise.all(data.map(current => this.create(current, params)));
    }
    const record = await this.model.create(data);
    return this.jsonify(record);
  }

  async update(id, data, params) {
    const query = {[this.hashKey]: id, ...params.query};
    await this.model.delete(query);
    await this.model.create(query);
    const result = await this.model.update(query, data);
    return this.jsonify(result);
  }

  async patch(id, data, params) {
    const query = {[this.hashKey]: id, ...params.query};
    const result = await this.model.update(query, data);
    return this.jsonify(result);
  }

  async remove(id, params) {
    const query = {[this.hashKey]: id, ...params.query};
    const result = await this.model.delete(query);
    return this.jsonify(result);
  }
}

export default (
  options,
  dynamooseOptions = DEFAULT_DYNAMOOSE_OPTIONS
) => new Service(options, dynamooseOptions);
