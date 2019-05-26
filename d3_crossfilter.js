////////////////////////////////////////////////////////////////////
// Contains a class using crossfilter to prepare data required for viz
//////////////////////////////////////////////////////////////////////

var CF_Dataset = function(opt){
  this.masterDataset = opt.data;
  this.cfDataset = crossfilter(this.masterDataset);
  this.dimOutput = opt.dimOutput;
  this.dimensions = opt.dimensions;
  this.measures = opt.measures;
  this.agg = opt.agg;
  this.cfDimensions = {};
  this.cfDimensionsVal = {};

  this.createInitialDimensions();

};

CF_Dataset.prototype.reduceAdd = function(attr){
   return function (p,v){
    //find the measure and aggregate function
    attr.map(function(d){
      p[d+'_SUM'] = parseFloat(p[d+'_SUM']) + parseFloat(v[d]);
      p[d+'_COUNT'] = p[d+'_COUNT'] + 1;
      p[d+'_AVG'] = p[d+'_SUM']/p[d+'_COUNT'];
      p[d+'_MAX'] = Math.max(p[d+'_MAX'],v[d]);
    })
    return p;
  };
};

CF_Dataset.prototype.reduceRemove = function(attr) {  return function(p,v){
    attr.map(function(d){
    p[d+'_SUM'] = parseFloat(p[d+'_SUM']) - parseFloat(v[d]);
    p[d+'_COUNT'] = p[d+'_COUNT'] - 1;
    })
    return p;
  };
};

CF_Dataset.prototype.reduceInitial = function(attr){
  return function(p,v){
    //create the starting dict
    var p = {};

    //populate dynamically with 0s for starting values for each of the agg functions
    attr.map(function(d){
      p[d+'_SUM'] = 0;
      p[d+'_COUNT'] = 0;
      p[d+'_AVG'] = 0;
      p[d+'_MAX'] = 0;
    })

      return p
    };
};


CF_Dataset.prototype.createInitialDimensions = function (){
  //create a crossfilter dimension for every element in the array and store in dictionary with the column name as the key
  var that = this;

  this.dimensions.map(function(d){
    var dimName = d;
    var cfDim = that.cfDataset.dimension(function(d){return d[dimName]});
    that.cfDimensions[dimName] = cfDim;
    });

    this.processData();
};

CF_Dataset.prototype.processData = function(){

  // for each dimension in dictionary, aggregate with measures creating new Dimensions Val
  for (const [key,value] of Object.entries(this.cfDimensions)){
    this.cfDimensionsVal[key] = value.group()
    .reduce(this.reduceAdd(this.measures),this.reduceRemove(this.measures),this.reduceInitial(this.measures))
    .top(Infinity);
  };

  this.cleanData();

};

CF_Dataset.prototype.cleanData = function(){

  //take dimOutput and aggregate based on specified method

  var that= this;

  this.dimOutput.map(function(d){

    var measure_agg = d.measure+d.agg;

    that.cfDimensionsVal[d.dim].map(function(d){
      d.value = d.value[measure_agg];
    });

  })

}

CF_Dataset.prototype.filterData = function(dim,val){
  this.cfDimensions[dim].filter(val);
  //this.cfDimensions['COFFEE_NM'].filter("Hambela");
  this.processData();
};
