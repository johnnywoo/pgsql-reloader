<?php

namespace PgsqlReloader;

class DatabaseDumpManager
{
    /** @var PostgresReloader */
    protected $reloader;

    protected $restoreNumber = 0;

    public function __construct($dbUri)
    {
        $this->reloader = new PostgresReloader($dbUri);
        $this->reloader->saveProduction();

        register_shutdown_function(function () {
            $this->reloader->restoreProduction();
        });
    }

    protected function initDatabase()
    {
        // override this to initialize your database
    }

    public function clean($saveCleanState = true)
    {
        if ($saveCleanState) {
            $this->restore('clean', function () {
                $this->clean(false);
            });
            return;
        }

        $this->restoreNumber++;
        $this->cleanMemcache();
        $this->initDatabase();
    }

    public function restore($name, callable $stateDescription)
    {
        $this->restoreNumber++;

        if ($this->reloader->restore($name)) {
            return;
        }

        $currentLevel = $this->restoreNumber;

        call_user_func($stateDescription);

        if ($currentLevel == $this->restoreNumber) {
            throw new \LogicException("restore() callback for '{$name}' needs a parent restore or a clean(), otherwise you will save a dirty database state");
        }

        $this->reloader->save($name);
    }
}
