<?php
//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------

use PHPUnit\Framework\TestCase;
use \Jaypha\ArrayTable;

class ArrayTableTest extends TestCase
{
  protected $table1, $table2, $table3;

  function setUp()
  {
    $this->table1 = new ArrayTable([
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40]),
      ['a' => 11, 'b' => 16,'c' => 31,'d' => 41],
      new ArrayTable(['a' => 11, 'b' => 28,'c' => 31,'d' => 41]),
      ['a' => 12, 'b' => 30,'c' => 31,'d' => 41],
      new ArrayTable(['a' => 15, 'b' => 42,'c' => 31,'d' => 41])
    ]);

    $this->table2 = [
      ['e' => 100, 'f' => 12, 'j' => 44 ],
      new ArrayTable(['e' => 101, 'f' => 13, 'j' => 344 ]),
      ['e' => 310, 'f' => 10, 'j' => 44 ],
      new ArrayTable(['e' => 410, 'f' => 11, 'j' => 10 ]),
      ['e' => 510, 'f' => 10, 'j' => 45 ],
    ];

    $this->table3 = new ArrayTable([
      ['d' => 100, 'f' => 12, 'j' => 54 ],
      new ArrayTable(['d' => 101, 'f' => 13, 'j' => 344 ]),
      ['d' => 310, 'f' => 10, 'j' => 44 ],
      new ArrayTable(['d' => 410, 'f' => 11, 'j' => 10 ]),
      ['d' => 510, 'f' => 10, 'j' => 45 ],
    ]);
  }

  function testExtractColumn()
  {
    $col = $this->table1->extractColumn("b");
    $this->assertEquals($col, [20,16,28,30,42]);
    $col = $this->table1->extractColumn("a");
    $this->assertEquals($col, [10,11,11,12,15]);
  }

  function testRemoveColumn()
  {
    $newTable = new ArrayTable([
      new ArrayTable(['a' => 10,'c' => 30,'d' => 40]),
      ['a' => 11,'c' => 31,'d' => 41],
      new ArrayTable(['a' => 11,'c' => 31,'d' => 41]),
      ['a' => 12,'c' => 31,'d' => 41],
      new ArrayTable(['a' => 15,'c' => 31,'d' => 41])
    ]);

    $this->table1->removeColumn("b");
    $this->assertEquals($this->table1, $newTable);
  }

  function testLeftJoin()
  {
    $newTable = new ArrayTable([
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'e' => 310, 'f' => 10, 'j' => 44]),
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'e' => 510, 'f' => 10, 'j' => 45]),
      ['a' => 11, 'b' => 16,'c' => 31,'d' => 41, 'e' => 410, 'f' => 11, 'j' => 10],
      new ArrayTable(['a' => 11, 'b' => 28,'c' => 31,'d' => 41, 'e' => 410, 'f' => 11, 'j' => 10]),
      ['a' => 12, 'b' => 30,'c' => 31,'d' => 41, 'e' => 100, 'f' => 12, 'j' => 44],
      new ArrayTable(['a' => 15, 'b' => 42,'c' => 31,'d' => 41])
    ]);
    $joined = $this->table1->leftJoin($this->table2,'a','f');

    $this->assertEquals($joined, $newTable);
  }

  function testInnerJoin()
  {
    $newTable = new ArrayTable([
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'e' => 310, 'f' => 10, 'j' => 44]),
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'e' => 510, 'f' => 10, 'j' => 45]),
      ['a' => 11, 'b' => 16,'c' => 31,'d' => 41, 'e' => 410, 'f' => 11, 'j' => 10],
      new ArrayTable(['a' => 11, 'b' => 28,'c' => 31,'d' => 41, 'e' => 410, 'f' => 11, 'j' => 10]),
      ['a' => 12, 'b' => 30,'c' => 31,'d' => 41, 'e' => 100, 'f' => 12, 'j' => 44],
    ]);
    $joined = $this->table1->innerJoin($this->table2,'a','f');
    $this->assertEquals($joined, $newTable);
  }

  function testFilteredLeftJoin()
  {
    $newTable = new ArrayTable([
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'f' => 10, 'j' => 44]),
      new ArrayTable(['a' => 10, 'b' => 20,'c' => 30,'d' => 40, 'f' => 10, 'j' => 45]),
      ['a' => 11, 'b' => 16,'c' => 31,'d' => 41, 'f' => 11, 'j' => 10],
      new ArrayTable(['a' => 11, 'b' => 28,'c' => 31,'d' => 41, 'f' => 11, 'j' => 10]),
      ['a' => 12, 'b' => 30,'c' => 31,'d' => 41, 'f' => 12, 'j' => 54],
      new ArrayTable(['a' => 15, 'b' => 42,'c' => 31,'d' => 41])
    ]);
    $callable = function($v,$i) {return $i != 'd';};
    $joined = $this->table1->filteredLeftJoin($callable, $this->table3,'a','f');
    $this->assertEquals($joined, $newTable);
  }
}

//----------------------------------------------------------------------------
// Copyright (C) 2018 Jaypha.
// License: BSL-1.0
// Author: Jason den Dulk
//
