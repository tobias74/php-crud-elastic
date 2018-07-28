<?php

namespace PhpCrudElastic;


class ElasticsearchFilterCriteriaVisitor
{
  protected $clauseParts = array();

  public function __construct($mapper)
  {
    $this->mapper = $mapper;
  }
  
  protected function getMapper()
  {
    return $this->mapper;    
  }
  
  
  public function visitAnyCriteria($anyCriteria)
  {
    $whereArray = array(
      'bool' => array(
        'must' => array()
      )
    );
    $this->setArrayForCriteria($anyCriteria, $whereArray);
  }

  public function visitAndCriteria($andCriteria)
  {
    $firstArray = $this->getArrayForCriteria($andCriteria->getFirstCriteria());
    $secondArray = $this->getArrayForCriteria($andCriteria->getSecondCriteria());

    $whereArray = array(
      'bool' => array(
        'must' => array($firstArray, $secondArray)
      )
    );

    $this->setArrayForCriteria($andCriteria, $whereArray);
  }
  
  public function visitOrCriteria($orCriteria)
  {
    $firstArray = $this->getArrayForCriteria($orCriteria->getFirstCriteria());
    $secondArray = $this->getArrayForCriteria($orCriteria->getSecondCriteria());
   
    $whereArray = array(
      'bool' => array(
        'should' => array($firstArray, $secondArray),
        'minimum_should_match' => 1
      )
    );

    $this->setArrayForCriteria($orCriteria, $whereArray);
  }

  
  public function visitEqualCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('term' => array($column => $criteria->getValue()));
    $this->setArrayForCriteria($criteria, $comp);
  }
  
  public function visitExistsCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('exists' => array('field' => $column));
    $this->setArrayForCriteria($criteria, $comp);
  }


  public function visitGreaterThanCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('range' => array($column => array(
      'gt' => $criteria->getValue()
    )));
    $this->setArrayForCriteria($criteria, $comp);
  }


  public function visitGreaterOrEqualCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('range' => array($column => array(
      'gte' => $criteria->getValue()
    )));
    $this->setArrayForCriteria($criteria, $comp);
  }

  
  public function visitLessThanCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('range' => array($column => array(
      'lt' => $criteria->getValue()
    )));
    $this->setArrayForCriteria($criteria, $comp);
  }
    
    
  public function visitLessOrEqualCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('range' => array($column => array(
      'lte' => $criteria->getValue()
    )));
    $this->setArrayForCriteria($criteria, $comp);
  }

        
  public function visitNotEqualCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('bool' => array('must_not' => array('term' => array($column => $criteria->getValue())) ) );
    $this->setArrayForCriteria($criteria, $comp);
  }
    
        
  public function visitCriteriaBetween($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getField());
    $comp = array('range' => array($column => array(
      'gt' => $criteria->getStartValue(),
      'lt' => $criteria->getEndValue()
    )));
    $this->setArrayForCriteria($criteria, $comp);
  }
  
  public function visitNotCriteria($criteria)
  {
    $comp = array('bool' => array('must_not' => $this->getArrayForCriteria($criteria->getNestedCriteria()) ) );
    $this->setArrayForCriteria($criteria, $comp);
  }
  
  
  public function visitWithinDistanceCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getGeometryField());
    $comp = array(
      'geo_distance' => array(
        'distance' => floatval($criteria->getMaximumDistance()),
        $column => array(
          'lat' => floatval($criteria->getLatitude()),
          'lon' => floatval($criteria->getLongitude())
        )
      )
    );
    $this->setArrayForCriteria($criteria, $comp);
  }

  public function visitGeoShapeCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getGeometryField());
    $comp = array(
      'geo_shape' => array(
        $column => $criteria->getGeoShapeDefinition()
      )
    );
    $this->setArrayForCriteria($criteria, $comp);
  }

  
  public function visitWithinBoundingBoxCriteria($criteria)
  {
    $column = $this->getMapper()->getColumnForField($criteria->getGeometryField());
    $comp = array(
      'geo_bounding_box' => array(
        $column => array(
          'top_left' => array(
            'lat' => floatval($criteria->topLeftLatitude),
            'lon' => floatval($criteria->topLeftLongitude)
          ),
          'bottom_right' => array(
            'lat' => floatval($criteria->bottomRightLatitude),
            'lon' => floatval($criteria->bottomRightLongitude)
          )
        )
      )
    );
    $this->setArrayForCriteria($criteria, $comp);
  }
  
  public function getArrayForCriteria($criteria)
  {
    return $this->clauseParts[$criteria->getKey()];
  }

  protected function setArrayForCriteria($criteria,$clause)
  {
    $this->clauseParts[$criteria->getKey()] = $clause;
  }
    
}

